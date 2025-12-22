package bench

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"slices"
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
	latencies []uint64 // in nanoseconds
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

// NewBenchmark initializes a Benchmark. After creating a bench call AddSample.
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
	slices.Sort(bm.SampleGroup.latencies)
	bm.latencies = bm.SampleGroup.latencies
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
	if len(s.latencies) == 0 {
		return fmt.Sprintf("%s msgs/sec ~ %s/sec", rate, throughput)
	} else {
		return fmt.Sprintf("%s msgs/sec ~ %s/sec ~ min: %sus ~ avg: %sus ~ max: %sus ~ P50: %sus ~ P90: %sus ~ P99: %sus ~ P99.9: %sus", rate, throughput, humanize.CommafWithDigits(MinLatency(s.latencies), 2), humanize.CommafWithDigits(AvgLatency(s.latencies), 2), humanize.CommafWithDigits(MaxLatency(s.latencies), 2), humanize.CommafWithDigits(PercentileLatency(s.latencies, 50), 2), humanize.CommafWithDigits(PercentileLatency(s.latencies, 90), 2), humanize.CommafWithDigits(PercentileLatency(s.latencies, 99), 2), humanize.CommafWithDigits(PercentileLatency(s.latencies, 99.9), 2))
	}
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
		if len(bm.SampleGroup.latencies) > 0 {
			buffer.WriteString(fmt.Sprintf("%s latencies per operation %s\n", indent, bm.SampleGroup.LatencyStatistics()))
		}
	}

	return buffer.String()
}

// CSV generates a csv report of all the samples collected
func (bm *BenchmarkResults) CSV() string {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	var headers []string
	if len(bm.latencies) == 0 {
		headers = []string{"#RunID", "ClientID", "MsgCount", "MsgBytes", "MsgsPerSec", "BytesPerSec", "DurationSecs"}
	} else {
		headers = []string{"#RunID", "ClientID", "MsgCount", "MsgBytes", "MsgsPerSec", "BytesPerSec", "DurationSecs", "MinLatencyMicroSecs", "AvgLatencyMicroSecs", "MaxLatencyMicroSecs", "P50LatencyMicroSecs", "P90LatencyMicroSecs", "P99LatencyMicroSecs", "P99.9LatencyMicroSecs", "StdDevLatencyMicroSecs"}
	}
	if err := writer.Write(headers); err != nil {
		log.Fatalf("Error while serializing headers %q: %v", headers, err)
	}

	pre := bm.Prefix()

	for j, c := range bm.SampleGroup.samples {
		var r []string

		if len(bm.latencies) == 0 {
			r = []string{bm.RunID, fmt.Sprintf("%s%d", pre, j), fmt.Sprintf("%d", c.msgCnt), fmt.Sprintf("%d", c.msgBytes), fmt.Sprintf("%d", c.Rate()), fmt.Sprintf("%f", c.Throughput()), fmt.Sprintf("%f", c.Duration().Seconds())}
		} else {
			r = []string{bm.RunID, fmt.Sprintf("%s%d", pre, j), fmt.Sprintf("%d", c.msgCnt), fmt.Sprintf("%d", c.msgBytes), fmt.Sprintf("%d", c.Rate()), fmt.Sprintf("%f", c.Throughput()), fmt.Sprintf("%f", c.Duration().Seconds()), fmt.Sprintf("%f", MinLatency(c.latencies)), fmt.Sprintf("%f", AvgLatency(c.latencies)), fmt.Sprintf("%f", MaxLatency(c.latencies)), fmt.Sprintf("%f", PercentileLatency(c.latencies, 50)), fmt.Sprintf("%f", PercentileLatency(c.latencies, 90)), fmt.Sprintf("%f", PercentileLatency(c.latencies, 99)), fmt.Sprintf("%f", PercentileLatency(c.latencies, 99.9)), fmt.Sprintf("%f", StdLatency(c.latencies))}
		}

		if err := writer.Write(r); err != nil {
			log.Fatalf("Error while serializing %v: %v", c, err)
		}
	}

	writer.Flush()
	return buffer.String()
}

// NewSample creates a new BenchSample initialized to the provided values. The nats.Conn information captured
func NewSample(jobCount int, msgSize int, start, end time.Time, latencies []uint64, nc *nats.Conn) *BenchSample {
	slices.Sort(latencies)
	s := BenchSample{jobMsgCnt: jobCount, start: start, end: end, latencies: latencies}
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

// MinLatency returns the minimum latency in microseconds
func MinLatency(latencies []uint64) float64 {
	if len(latencies) == 0 {
		return 0
	}

	return float64(latencies[0]) / 1000.0
}

// MaxLatency returns the maximum latency in microseconds
func MaxLatency(latencies []uint64) float64 {
	if len(latencies) == 0 {
		return 0
	}

	return float64(latencies[len(latencies)-1]) / 1000.0
}

// AvgLatency returns the average latency in microseconds
func AvgLatency(latencies []uint64) float64 {
	if len(latencies) == 0 {
		return 0
	}

	sum := uint64(0)
	for _, l := range latencies {
		sum += l
	}

	return (float64(sum) / float64(len(latencies))) / 1000.0
}

// PercentileLatency returns the given percentile latency in microseconds
func PercentileLatency(latencies []uint64, percentile float64) float64 {
	if len(latencies) == 0 {
		return 0
	}

	index := int(math.Ceil((percentile/100)*float64(len(latencies)))) - 1
	return float64(latencies[index]) / 1000.0
}

// StdLatency returns the standard deviation of the latencies in microseconds
func StdLatency(latencies []uint64) float64 {
	{
		if len(latencies) == 0 {
			return 0
		}

		avg := AvgLatency(latencies)
		sum := float64(0)

		for _, c := range latencies {
			sum += math.Pow(float64(c)-avg, 2)
		}

		variance := sum / float64(len(latencies))
		return math.Sqrt(variance) / 1000.0
	}
}

// NewSampleGroup initializer
func NewSampleGroup() *BenchSampleGroup {
	s := new(BenchSampleGroup)
	s.samples = make([]*BenchSample, 0)
	return s
}

// MsgRateStatistics prints statistics for the message rates of the BenchSample group (min, average, max and standard deviation)
func (sg *BenchSampleGroup) MsgRateStatistics() string {
	return fmt.Sprintf("min %s | avg %s | max %s | stddev %s msgs", humanize.Comma(sg.MinMsgRate()), humanize.Comma(sg.AvgMsgRate()), humanize.Comma(sg.MaxMsgRate()), humanize.Comma(int64(sg.StdDevMsgs())))
}

// LatencyStatistics prints statistics for the average latencies in microseconds of the BenchSample group (min, average, max and standard deviation)
func (sg *BenchSampleGroup) LatencyStatistics() string {
	return fmt.Sprintf("min %sus | avg %sus | max %sus | stddev %sus | P50 %sus | P90 %sus | P99 %sus | P99.9: %sus", humanize.CommafWithDigits(MinLatency(sg.latencies), 2), humanize.CommafWithDigits(AvgLatency(sg.latencies), 2), humanize.CommafWithDigits(MaxLatency(sg.latencies), 2), humanize.CommafWithDigits(StdLatency(sg.latencies), 2), humanize.CommafWithDigits(PercentileLatency(sg.latencies, 50), 2), humanize.CommafWithDigits(PercentileLatency(sg.latencies, 90), 2), humanize.CommafWithDigits(PercentileLatency(sg.latencies, 99), 2), humanize.CommafWithDigits(PercentileLatency(sg.latencies, 99.9), 2))
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
func (sg *BenchSampleGroup) MinLatency() uint64 {
	if len(sg.latencies) == 0 {
		return 0
	}

	return sg.latencies[0]
}

// MaxLatency returns the largest average latency in microseconds in the SampleGroup
func (sg *BenchSampleGroup) MaxLatency() uint64 {
	if len(sg.latencies) == 0 {
		return 0
	}

	return sg.latencies[len(sg.latencies)-1]
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

// StdDevMsgs returns the standard deviation the message rates in the SampleGroup
func (sg *BenchSampleGroup) StdDevMsgs() float64 {
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
	sg.latencies = append(sg.latencies, e.latencies...)

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
