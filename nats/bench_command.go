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
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
	"gopkg.in/alecthomas/kingpin.v2"
)

type benchCmd struct {
	subject  string
	numPubs  int
	numSubs  int
	numMsg   int
	msgSize  int
	csvFile  string
	progress bool
	ack      bool
}

func configureBenchCommand(app *kingpin.Application) {
	c := &benchCmd{}
	bench := app.Command("bench", "Benchmark utility").Action(c.bench)
	bench.Arg("subject", "Subject to use for testing").Required().StringVar(&c.subject)
	bench.Flag("pub", "Number of concurrent publishers").Default("1").IntVar(&c.numPubs)
	bench.Flag("sub", "Number of concurrent subscribers").Default("0").IntVar(&c.numSubs)
	bench.Flag("msgs", "Number of messages to publish").Default("100000").IntVar(&c.numMsg)
	bench.Flag("size", "Size of the test messages").Default("128").IntVar(&c.msgSize)
	bench.Flag("csv", "Save benchmark data to CSV file").StringVar(&c.csvFile)
	bench.Flag("progress", "Enable progress bar while publishing").Default("true").BoolVar(&c.progress)
	bench.Flag("ack", "Waits for acknowledgement on messages using Requests rather than Publish").Default("false").BoolVar(&c.ack)

	cheats["bench"] = `# benchmark JetStream acknowledged publishes
nats bench --ack --msgs 10000 ORDERS.bench

# benchmark core nats publish and subscribe with 10 publishers and subscribers
nats bench --pub 10 --sub 10 --msgs 10000 --size 512
`
}

func (c *benchCmd) bench(_ *kingpin.ParseContext) error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	log.Printf("Starting benchmark [msgs=%s, msgsize=%s, pubs=%d, subs=%d]", humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs)

	if c.ack && c.progress {
		log.Printf("Disabling progress bars in request mode")
		c.progress = false
	}

	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runSubscriber(bm, nc, startwg, donewg)
	}
	startwg.Wait()

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numPubs)
	for i := 0; i < c.numPubs; i++ {
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runPublisher(bm, nc, startwg, donewg, pubCounts[i])
	}

	if c.progress {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	if c.progress {
		uiprogress.Stop()
	}

	fmt.Println()
	fmt.Println(bm.Report())

	if c.csvFile != "" {
		csv := bm.CSV()
		ioutil.WriteFile(c.csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", c.csvFile)
	}

	return nil
}

func (c *benchCmd) runPublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	startwg.Done()

	var progress *uiprogress.Bar
	if c.progress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	} else {
		log.Printf("Starting publisher, publishing %s messages", humanize.Comma(int64(numMsg)))
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	start := time.Now()

	var m *nats.Msg
	var err error

	errBytes := []byte("error")
	minusByte := byte('-')

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		if !c.ack {
			nc.Publish(c.subject, msg)
			continue
		}

		m, err = nc.Request(c.subject, msg, time.Second)
		if err != nil {
			log.Println(err)
			continue
		}

		if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
			log.Printf("Did not receive a positive ACK: %q", m.Data)
		}
	}

	nc.Flush()

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup) {
	received := 0
	ch := make(chan time.Time, 2)

	sub, _ := nc.Subscribe(c.subject, func(msg *nats.Msg) {
		received++
		if received == 1 {
			ch <- time.Now()
		}
		if received >= c.numMsg {
			ch <- time.Now()
		}
	})

	sub.SetPendingLimits(-1, -1)
	nc.Flush()
	startwg.Done()

	start := <-ch
	end := <-ch

	bm.AddSubSample(bench.NewSample(c.numMsg, c.msgSize, start, end, nc))

	nc.Close()
	donewg.Done()
}
