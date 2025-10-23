package bench

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// A BenchSample for a particular client
type BenchSample struct {
	jobMsgCnt int
	msgCnt    uint64
	msgBytes  uint64
	iOBytes   uint64
	start     time.Time
	end       time.Time
}

// BenchSampleGroup for a number of samples, the group is a BenchSample itself aggregating the values the samples
type BenchSampleGroup struct {
	BenchSample
	samples []*BenchSample
}

// BenchmarkResults to hold the various Samples organized by publishers and subscribers
type BenchmarkResults struct {
	BenchSample
	Name           string
	RunID          string
	BenchType      string
	SampleGroup    *BenchSampleGroup
	SamplesChannel chan *BenchSample
}

// Benchmark stats
// NewBenchmark initializes a Benchmark. After creating a bench call AddSubSample/AddPubSample.
// When done collecting samples, call Close() to calculate aggregates.
func NewBenchmark(name string, benchType string, clientCount int) *BenchmarkResults {
	bm := BenchmarkResults{Name: name, RunID: nuid.Next()}
	bm.SampleGroup = NewSampleGroup()
	bm.SamplesChannel = make(chan *BenchSample, clientCount)
	bm.BenchType = benchType
	return &bm
}

// Close organizes collected Samples and calculates aggregates. After Close(), no more samples can be added.
func (bm *BenchmarkResults) Close() {
	close(bm.SamplesChannel)

	for s := range bm.SamplesChannel {
		bm.SampleGroup.AddSample(s)
	}

	bm.start = bm.SampleGroup.start
	bm.end = bm.SampleGroup.end

	bm.msgBytes = bm.SampleGroup.msgBytes
	bm.iOBytes = bm.SampleGroup.iOBytes
	bm.msgCnt = bm.SampleGroup.msgCnt
	bm.jobMsgCnt = bm.SampleGroup.jobMsgCnt
}

// AddSample adds a BenchSample to the BenchmarkResults
func (bm *BenchmarkResults) AddSample(s *BenchSample) {
	bm.SamplesChannel <- s
}

// Prefix generates the "P" or "S" prefix for the BenchmarkResults type
func (bm *BenchmarkResults) Prefix() string {
	switch bm.BenchType {
	case TypeCorePub, TypeJSPubAsync, TypeJSPubBatch, TypeJSPubSync, TypeKVPut, TypeServiceRequest:
		return "P"
	case TypeCoreSub, TypeJSConsume, TypeJSFetch, TypeJSOrdered, TypeJSGetSync, TypeJSGetDirectBatched, TypeKVGet, TypeOldJSPush, TypeOldJSPull, TypeOldJSOrdered:
		return "S"
	case TypeServiceServe:
		return "?" // at this time service servers never complete and do not produce samples
	default:
		return "?"
	}
}

// String generates a human-readable report of the BenchSample
func (s *BenchSample) String() string {
	rate := humanize.Comma(s.Rate())
	throughput := humanize.IBytes(uint64(s.Throughput()))

	return fmt.Sprintf("%s msgs/sec ~ %s/sec ~ %sus", rate, throughput, fmt.Sprintf("%.2f", s.AvgLatency()))
}

// Report returns a human-readable report of the samples taken in the Benchmark
func (bm *BenchmarkResults) Report() string {
	var buffer bytes.Buffer

	indent := ""

	if bm.Prefix() == "?" || !bm.SampleGroup.HasSamples() {
		return "No publisher or subscribers. Nothing to report."
	}

	if len(bm.SampleGroup.samples) == 1 {
		buffer.WriteString(fmt.Sprintf("%s %s stats: %s\n", bm.Name, GetBenchTypeLabel(bm.BenchType), bm))
		indent += " "
	} else {
		for i, stat := range bm.SampleGroup.samples {
			buffer.WriteString(fmt.Sprintf("%s [%d] %v (%s msgs)\n", indent+" ", i+1, stat, humanize.Comma(int64(stat.jobMsgCnt))))
		}
		buffer.WriteString(fmt.Sprintf("\n%s %s %s aggregated stats: %s\n", indent, bm.Name, GetBenchTypeLabel(bm.BenchType), fmt.Sprintf("%s msgs/sec ~ %s/sec", humanize.Comma(bm.SampleGroup.Rate()), humanize.IBytes(uint64(bm.SampleGroup.Throughput())))))
		buffer.WriteString(fmt.Sprintf("%s message rates %s\n", indent, bm.SampleGroup.MsgRateStatistics()))
		buffer.WriteString(fmt.Sprintf("%s avg latencies %s\n", indent, bm.SampleGroup.LatencyStatistics()))
	}

	return buffer.String()
}

// CSV generates a csv report of all the samples collected
func (bm *BenchmarkResults) CSV() string {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	headers := []string{"#RunID", "ClientID", "MsgCount", "MsgBytes", "MsgsPerSec", "BytesPerSec", "DurationSecs", "AvgLatencyMicroSecs"}
	if err := writer.Write(headers); err != nil {
		log.Fatalf("Error while serializing headers %q: %v", headers, err)
	}

	pre := bm.Prefix()

	for j, c := range bm.SampleGroup.samples {
		r := []string{bm.RunID, fmt.Sprintf("%s%d", pre, j), fmt.Sprintf("%d", c.msgCnt), fmt.Sprintf("%d", c.msgBytes), fmt.Sprintf("%d", c.Rate()), fmt.Sprintf("%f", c.Throughput()), fmt.Sprintf("%f", c.Duration().Seconds()), fmt.Sprintf("%f", c.AvgLatency())}
		if err := writer.Write(r); err != nil {
			log.Fatalf("Error while serializing %v: %v", c, err)
		}
	}

	writer.Flush()
	return buffer.String()
}

// NewSample creates a new BenchSample initialized to the provided values. The nats.Conn information captured
func NewSample(jobCount int, msgSize int, start, end time.Time, nc *nats.Conn) *BenchSample {
	s := BenchSample{jobMsgCnt: jobCount, start: start, end: end}
	s.msgBytes = uint64(msgSize * jobCount)
	s.msgCnt = nc.OutMsgs + nc.InMsgs
	s.iOBytes = nc.OutBytes + nc.InBytes
	return &s
}

// Throughput of bytes per second
func (s *BenchSample) Throughput() float64 {
	return float64(s.msgBytes) / s.Duration().Seconds()
}

// Rate of messages in the job per second
func (s *BenchSample) Rate() int64 {
	return int64(float64(s.jobMsgCnt) / s.Duration().Seconds())
}

// Duration that the BenchSample was active
func (s *BenchSample) Duration() time.Duration {
	return s.end.Sub(s.start)
}

// AvgLatency returns the average latency in microseconds
func (s *BenchSample) AvgLatency() float64 {
	if s.jobMsgCnt == 0 {
		return 0
	}

	return s.Duration().Seconds() / float64(s.jobMsgCnt) * 1_000_000
}

// NewSampleGroup initializer
func NewSampleGroup() *BenchSampleGroup {
	s := new(BenchSampleGroup)
	s.samples = make([]*BenchSample, 0)
	return s
}

// MsgRateStatistics prints statistics for the message rates of the BenchSample group (min, average, max and standard deviation)
func (sg *BenchSampleGroup) MsgRateStatistics() string {
	return fmt.Sprintf("min %s | avg %s | max %s | stddev %s msgs", humanize.Comma(sg.MinMsgRate()), humanize.Comma(sg.AvgMsgRate()), humanize.Comma(sg.MaxMsgRate()), humanize.Comma(int64(sg.StdMsgDev())))
}

// LatencyStatistics prints statistics for the average latencies in microseconds of the BenchSample group (min, average, max and standard deviation)
func (sg *BenchSampleGroup) LatencyStatistics() string {
	return fmt.Sprintf("min %.2fus | avg %.2fus | max %.2fus | stddev %.2fus", sg.MinLatency(), sg.avgLatency(), sg.MaxLatency(), sg.StdLatency())
}

// MinMsgRate returns the smallest message Rate in the SampleGroup
func (sg *BenchSampleGroup) MinMsgRate() int64 {
	m := int64(0)

	for i, s := range sg.samples {
		if i == 0 {
			m = s.Rate()
		}

		m = min(m, s.Rate())
	}

	return m
}

// MinLatency returns the smallest average latency in microseconds in the SampleGroup
func (sg *BenchSampleGroup) MinLatency() float64 {
	m := float64(0)

	for i, s := range sg.samples {
		if i == 0 {
			m = s.AvgLatency()
		}

		m = math.Min(m, s.AvgLatency())
	}

	return m
}

// MaxMsgRate returns the largest message Rate in the SampleGroup
func (sg *BenchSampleGroup) MaxMsgRate() int64 {
	m := int64(0)

	for i, s := range sg.samples {
		if i == 0 {
			m = s.Rate()
		}

		m = max(m, s.Rate())
	}

	return m
}

// MaxLatency returns the largest average latency in microseconds in the SampleGroup
func (sg *BenchSampleGroup) MaxLatency() float64 {
	m := float64(0)
	for i, s := range sg.samples {
		if i == 0 {
			m = s.AvgLatency()
		}

		m = math.Max(m, s.AvgLatency())
	}

	return m
}

// AvgMsgRate returns the average of all the message rates in the SampleGroup
func (sg *BenchSampleGroup) AvgMsgRate() int64 {
	if !sg.HasSamples() {
		return 0
	}

	sum := uint64(0)

	for _, s := range sg.samples {
		sum += uint64(s.Rate())
	}

	return int64(sum / uint64(len(sg.samples)))
}

// avgLatency returns the average of all the average latencies in microseconds in the SampleGroup
func (sg *BenchSampleGroup) avgLatency() float64 {
	if !sg.HasSamples() {
		return 0
	}

	sum := float64(0)

	for _, s := range sg.samples {
		sum += s.AvgLatency()
	}

	return sum / float64(len(sg.samples))
}

// StdMsgDev returns the standard deviation the message rates in the SampleGroup
func (sg *BenchSampleGroup) StdMsgDev() float64 {
	if !sg.HasSamples() {
		return 0
	}

	avg := float64(sg.AvgMsgRate())
	sum := float64(0)

	for _, c := range sg.samples {
		sum += math.Pow(float64(c.Rate())-avg, 2)
	}

	variance := sum / float64(len(sg.samples))
	return math.Sqrt(variance)
}

// StdLatency returns the standard deviation of the average latencies in microseconds in the SampleGroup
func (sg *BenchSampleGroup) StdLatency() float64 {
	if !sg.HasSamples() {
		return 0
	}

	avg := sg.avgLatency()
	sum := float64(0)

	for _, c := range sg.samples {
		sum += math.Pow(c.AvgLatency()-avg, 2)
	}

	variance := sum / float64(len(sg.samples))
	return math.Sqrt(variance)
}

// AddSample adds a BenchSample to the SampleGroup. After adding a BenchSample it shouldn't be modified.
func (sg *BenchSampleGroup) AddSample(e *BenchSample) {
	sg.samples = append(sg.samples, e)

	if len(sg.samples) == 1 {
		sg.start = e.start
		sg.end = e.end
	}

	sg.iOBytes += e.iOBytes
	sg.jobMsgCnt += e.jobMsgCnt
	sg.msgCnt += e.msgCnt
	sg.msgBytes += e.msgBytes

	if e.start.Before(sg.start) {
		sg.start = e.start
	}

	if e.end.After(sg.end) {
		sg.end = e.end
	}
}

// HasSamples returns true if the group has samples
func (sg *BenchSampleGroup) HasSamples() bool {
	return len(sg.samples) > 0
}
