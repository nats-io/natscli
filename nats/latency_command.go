// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/nats-io/nats.go"
	histwriter "github.com/tylertreat/hdrhistogram-writer"
	"gopkg.in/alecthomas/kingpin.v2"
)

type latencyCmd struct {
	serverB       string
	targetPubRate int
	msgSize       int
	testDuration  time.Duration
	histFile      string
	numPubs       int
}

func configureLatencyCommand(app *kingpin.Application) {
	c := &latencyCmd{}

	latency := app.Command("latency", "Perform latency tests between two NATS servers").Alias("lat").Action(c.latencyAction)
	latency.Flag("server-b", "The second server to to subscribe on").Required().StringVar(&c.serverB)
	latency.Flag("size", "Message size").Default("8").IntVar(&c.msgSize)
	latency.Flag("rate", "Rate of messages per second").Default("1000").IntVar(&c.targetPubRate)
	latency.Flag("duration", "Test duration").Default("5s").DurationVar(&c.testDuration)
	latency.Flag("histogram", "Output file to store the histogram in").StringVar(&c.histFile)

	cheats["latency"] = `# To test latency between 2 servers
nats latency --server srv1.example.net:4222 --server-b srv2.example.net:4222 --duration 10s
`
}

func (c *latencyCmd) latencyAction(_ *kingpin.ParseContext) error {
	start := time.Now()
	c.numPubs = int(c.testDuration/time.Second) * c.targetPubRate
	log.SetFlags(0)

	if c.msgSize < 8 {
		return fmt.Errorf("message Payload Size must be at least %d bytes", 8)
	}

	c1, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}

	c2, err := newNatsConn(c.serverB, natsOpts()...)
	if err != nil {
		return err
	}

	// Do some quick RTT calculations
	log.Println("==============================")
	now := time.Now()
	c1.Flush()
	log.Printf("Pub Server RTT : %v\n", c.fmtDur(time.Since(now)))

	now = time.Now()
	c2.Flush()
	log.Printf("Sub Server RTT : %v\n", c.fmtDur(time.Since(now)))

	// Duration tracking
	durations := make([]time.Duration, 0, c.numPubs)

	// Wait for all messages to be received.
	var wg sync.WaitGroup
	wg.Add(1)

	// Random subject (to run multiple tests in parallel)
	subject := nats.NewInbox()

	// Count the messages.
	received := 0

	// Async Subscriber (Runs in its own Goroutine)
	c2.Subscribe(subject, func(msg *nats.Msg) {
		sendTime := int64(binary.LittleEndian.Uint64(msg.Data))
		durations = append(durations, time.Duration(time.Now().UnixNano()-sendTime))
		received++
		if received >= c.numPubs {
			wg.Done()
		}
	})
	// Make sure interest is set for subscribe before publish since a different connection.
	c2.Flush()

	// wait for routes to be established so we get every message
	c.waitForRoute(c1, c2)

	log.Printf("Message Payload: %v\n", c.byteSize(c.msgSize))
	log.Printf("Target Duration: %v\n", c.testDuration)
	log.Printf("Target Msgs/Sec: %v\n", c.targetPubRate)
	log.Printf("Target Band/Sec: %v\n", c.byteSize(c.targetPubRate*c.msgSize*2))
	log.Println("==============================")

	// Random payload
	data := make([]byte, c.msgSize)
	io.ReadFull(rand.Reader, data)

	// For publish throttling
	delay := time.Second / time.Duration(c.targetPubRate)
	pubStart := time.Now()

	// Throttle logic, crude I know, but works better then time.Ticker.
	adjustAndSleep := func(count int) {
		r := c.rps(count, time.Since(pubStart))
		adj := delay / 20 // 5%
		if adj == 0 {
			adj = 1 // 1ns min
		}
		if r < c.targetPubRate {
			delay -= adj
		} else if r > c.targetPubRate {
			delay += adj
		}
		if delay < 0 {
			delay = 0
		}
		time.Sleep(delay)
	}

	// Now publish
	for i := 0; i < c.numPubs; i++ {
		now := time.Now()
		// Place the send time in the front of the payload.
		binary.LittleEndian.PutUint64(data[0:], uint64(now.UnixNano()))
		c1.Publish(subject, data)
		adjustAndSleep(i + 1)
	}
	pubDur := time.Since(pubStart)
	wg.Wait()
	subDur := time.Since(pubStart)

	// If we are writing to files, save the original unsorted data
	if c.histFile != "" {
		if err := c.writeRawFile(c.histFile+".raw", durations); err != nil {
			log.Printf("Unable to write raw output file: %v", err)
		}
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	h := hdrhistogram.New(1, int64(durations[len(durations)-1]), 5)
	for _, d := range durations {
		h.RecordValue(int64(d))
	}

	log.Printf("HDR Percentiles:\n")
	log.Printf("10:       %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(10))))
	log.Printf("50:       %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(50))))
	log.Printf("75:       %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(75))))
	log.Printf("90:       %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(90))))
	log.Printf("99:       %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(99))))
	log.Printf("99.9:     %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(99.9))))
	log.Printf("99.99:    %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(99.99))))
	log.Printf("99.999:   %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(99.999))))
	log.Printf("99.9999:  %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(99.9999))))
	log.Printf("99.99999: %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(99.99999))))
	log.Printf("100:      %v\n", c.fmtDur(time.Duration(h.ValueAtQuantile(100.0))))
	log.Println("==============================")

	if c.histFile != "" {
		pctls := histwriter.Percentiles{10, 25, 50, 75, 90, 99, 99.9, 99.99, 99.999, 99.9999, 99.99999, 100.0}
		histwriter.WriteDistributionFile(h, pctls, 1.0/1000000.0, c.histFile+".histogram")
	}

	// Print results
	med, err := c.getMedian(durations)
	if err != nil {
		return err
	}

	log.Printf("Actual Msgs/Sec: %d\n", c.rps(c.numPubs, pubDur))
	log.Printf("Actual Band/Sec: %v\n", c.byteSize(c.rps(c.numPubs, pubDur)*c.msgSize*2))
	log.Printf("Minimum Latency: %v", c.fmtDur(durations[0]))
	log.Printf("Median Latency : %v", c.fmtDur(med))
	log.Printf("Maximum Latency: %v", c.fmtDur(durations[len(durations)-1]))
	log.Printf("1st Sent Wall Time : %v", c.fmtDur(pubStart.Sub(start)))
	log.Printf("Last Sent Wall Time: %v", c.fmtDur(pubDur))
	log.Printf("Last Recv Wall Time: %v", c.fmtDur(subDur))

	return nil
}

// Just pretty print the byte sizes.
func (c *latencyCmd) byteSize(n int) string {
	sizes := []string{"B", "K", "M", "G", "T"}
	base := float64(1024)
	if n < 10 {
		return fmt.Sprintf("%d%s", n, sizes[0])
	}
	e := math.Floor(c.logn(float64(n), base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(n)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}
	return fmt.Sprintf(f, val, suffix)
}

func (c *latencyCmd) logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

func (c *latencyCmd) getMedian(values []time.Duration) (time.Duration, error) {
	l := len(values)
	if l == 0 {
		return 0, fmt.Errorf("empty set")
	}
	if l%2 == 0 {
		return (values[l/2-1] + values[l/2]) / 2, nil
	}
	return values[l/2], nil
}

// waitForRoute tests a subscription in the server to ensure subject interest
// has been propagated between servers.  Otherwise, we may miss early messages
// when testing with clustered servers and the test will hang.
func (c *latencyCmd) waitForRoute(pnc *nats.Conn, snc *nats.Conn) error {
	// No need to continue if using one server
	if strings.Compare(pnc.ConnectedServerId(), snc.ConnectedServerId()) == 0 {
		return nil
	}

	// Setup a test subscription to let us know when a message has been received.
	// Use a new inbox subject as to not skew results
	var routed int32
	subject := nats.NewInbox()
	sub, err := snc.Subscribe(subject, func(msg *nats.Msg) {
		atomic.AddInt32(&routed, 1)
	})
	if err != nil {
		return fmt.Errorf("couldn't subscribe to test subject %s: %v", subject, err)
	}
	defer sub.Unsubscribe()
	snc.Flush()

	// Periodically send messages until the test subscription receives
	// a message.  Allow for two seconds.
	start := time.Now()
	for atomic.LoadInt32(&routed) == 0 {
		if time.Since(start) > (time.Second * 2) {
			return fmt.Errorf("couldn't receive end-to-end test message")
		}
		if err = pnc.Publish(subject, nil); err != nil {
			return fmt.Errorf("couldn't publish to test subject %s:  %v", subject, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

// Make time durations a bit prettier.
func (c *latencyCmd) fmtDur(t time.Duration) time.Duration {
	// e.g 234us, 4.567ms, 1.234567s
	return t.Truncate(time.Microsecond)
}

func (c *latencyCmd) rps(count int, elapsed time.Duration) int {
	return int(float64(count) / (float64(elapsed) / float64(time.Second)))
}

// writeRawFile creates a file with a list of recorded latency
// measurements, one per line.
func (c *latencyCmd) writeRawFile(filePath string, values []time.Duration) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, value := range values {
		fmt.Fprintf(f, "%f\n", float64(value.Nanoseconds())/1000000.0)
	}
	return nil
}
