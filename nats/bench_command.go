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
	subject        string
	numPubs        int
	numSubs        int
	numMsg         int
	msgSize        int
	csvFile        string
	progress       bool
	syncPub        bool
	js             bool
	pubBatch       int
	jsFile         bool
	pull           bool
	pullBatch      int
	ack            bool
	replicas       int
	noPurge        bool
	noDeleteStream bool
	maxAckPending  int
}

const (
	JS_STREAM_NAME       string = "benchstream"
	JS_PUSHCONSUMER_NAME string = "pushconsumer"
	JS_PULLCONSUMER_NAME string = "pullconsumer"
)

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
	bench.Flag("syncpub", "Synchronously waits for acknowledgement on published messages (using Request in NATS or synchronous Publish in JS)").Default("false").BoolVar(&c.syncPub)
	bench.Flag("js", "Use JetStream streaming").Default("false").BoolVar(&c.js)
	bench.Flag("pubbatch", "Sets the batch size for JS asynchronous publishing").Default("100").IntVar(&c.pubBatch)
	bench.Flag("jsfile", "Persist the stream to file").Default("false").BoolVar(&c.jsFile)
	bench.Flag("pull", "Uses a JS pull consumer").Default("false").BoolVar(&c.pull)
	bench.Flag("pullbatch", "Sets the batch size for the JS pull consumer").Default("100").IntVar(&c.pullBatch)
	bench.Flag("ack", "Acks consumption of messages").Default("false").BoolVar(&c.ack)
	bench.Flag("replicas", "Number of stream replicas").Default("1").IntVar(&c.replicas)
	bench.Flag("nopurge", "Do not purge the stream before running").Default("false").BoolVar(&c.noPurge)
	bench.Flag("nodelete", "Do not delete the stream at the end of the run").Default("false").BoolVar(&c.noDeleteStream)
	bench.Flag("maxackpending", "Max acks pending for JS consumer").Default("-1").IntVar(&c.maxAckPending)

	cheats["bench"] = `# benchmark JetStream acknowledged publishes
nats bench testsubject --syncpub --msgs=10000 ORDERS.bench

# benchmark core nats publish and subscribe with 10 publishers and subscribers
nats bench testsubject --pub=10 --sub=10 --msgs=10000 --size=512

# benchmark JS publish and push consumers at the same time
nats bench testsubject --pub=4 --sub=4 --js

# benchmark JS stream purge and async batched publish into the stream
nats bench testsubject --pub=4 --sub=0 --js --nodelete 

# benchmark JS stream do not purge and get replay from the stream using a push consumer
nats bench testsubject --pub=0 --sub=4 --js --nodelete --nopurge

# benchmark JS stream do not purge and get replay from the stream using a pull consumer
nats bench testsubject --pub=0 --sub=4 --js --nodelete --nopurge --pull
`
}

func (c *benchCmd) bench(_ *kingpin.ParseContext) error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	log.Printf("Starting benchmark [msgs=%s, msgsize=%s, pubs=%d, subs=%d, js=%v, jsfile=%v, syncpub=%v, pubbatch=%s, pull=%v, pullbatch=%s, ack=%v, maxackpending=%s, replicas=%d, nopurge=%v, nodelete=%v]", humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.js, c.jsFile, c.syncPub, humanize.Comma(int64(c.pubBatch)), c.pull, humanize.Comma(int64(c.pullBatch)), c.ack, humanize.Comma(int64(c.maxAckPending)), c.replicas, c.noPurge, c.noDeleteStream)

	if c.syncPub && c.progress {
		log.Printf("Disabling progress bars in syncpub mode")
		c.progress = false
	}

	if c.js && c.maxAckPending != -1 && c.maxAckPending < c.numMsg && !c.ack && c.numSubs > 0 {
		log.Printf("WARNING: max acks pending is smaller than the mumber of messages and the subscribers are not set to ack, use --ack or you run the risk of the consumers not receiving all the messages!")
	}

	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

	if c.js {
		// create the stream for the benchmark (and purge it)
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			log.Fatalf("nats connection failed: %s", err)
			return err
		}

		js, err = nc.JetStream()
		if err != nil {
			log.Fatalf("couldn't create jetstream context: %v", err)
		}

		storageType := func() nats.StorageType {
			if c.jsFile {
				return nats.FileStorage
			} else {
				return nats.MemoryStorage
			}
		}()

		js.AddStream(&nats.StreamConfig{Name: JS_STREAM_NAME, Subjects: []string{c.subject}, Retention: nats.LimitsPolicy, MaxConsumers: -1, MaxMsgs: -1, MaxBytes: -1, Discard: nats.DiscardOld, MaxAge: 9223372036854775807, MaxMsgsPerSubject: -1, MaxMsgSize: -1, Storage: storageType, Replicas: c.replicas, Duplicates: time.Second * 2})
		if !c.noPurge {
			log.Printf("Purging the stream")
			js.PurgeStream(JS_STREAM_NAME)
		}

		if !c.noDeleteStream {
			log.Printf("Will delete the stream at the end of the run")
			defer js.DeleteStream(JS_STREAM_NAME)
		}

		// create the pull consumer
		if c.pull && c.numSubs > 0 {
			js.AddConsumer(JS_STREAM_NAME, &nats.ConsumerConfig{
				Durable:       JS_PULLCONSUMER_NAME,
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				ReplayPolicy:  nats.ReplayInstantPolicy,
				MaxAckPending: c.maxAckPending,
			})
			defer js.DeleteConsumer(JS_STREAM_NAME, JS_PULLCONSUMER_NAME)
		}
	}
	subCounts := bench.MsgsPerClient(c.numMsg, c.numSubs)

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull {
				return subCounts[i]
			} else {
				return c.numMsg
			}
		}()

		go c.runSubscriber(bm, nc, startwg, donewg, numMsg)
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

	if !c.js {
		if !c.syncPub {
			for i := 0; i < numMsg; i++ {
				if progress != nil {
					progress.Incr()
				}
				err := nc.Publish(c.subject, msg)
				if err != nil {
					log.Fatalf("Publish error: %s", err)
				}
			}
		} else {
			var m *nats.Msg
			var err error

			errBytes := []byte("error")
			minusByte := byte('-')

			for i := 0; i < numMsg; i++ {
				if progress != nil {
					progress.Incr()
				}

				if !c.syncPub {
					err = nc.Publish(c.subject, msg)
					if err != nil {
						log.Fatalf("Publish error: %s", err)
					}
				}

				m, err = nc.Request(c.subject, msg, time.Second)
				if err != nil {
					log.Fatalf("Request error: %s", err)
				}

				if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
					log.Fatalf("Publish Request did not receive a positive ACK: %q", m.Data)
				}
			}
		}
	} else {
		js, err := nc.JetStream()
		if err != nil {
			log.Fatalf("couldn't create jetstream context: %v", err)
		}

		if !c.syncPub {
			for i := 0; i < numMsg; i += c.pubBatch {
				for j := 0; j < c.pubBatch && i+j < c.numMsg; j++ {
					if progress != nil {
						progress.Incr()
					}
					js.PublishAsync(c.subject, msg)
					if err != nil {
						log.Fatalf("PubAsync error: %v", err)
					}
				}

				select {
				case <-js.PublishAsyncComplete():
				case <-time.After(time.Second):
					log.Fatalf("JS PubAsync did not receive a positive ack")
				}
			}
		} else {
			for i := 0; i < numMsg; i++ {
				if progress != nil {
					progress.Incr()
				}
				_, err = js.Publish(c.subject, msg)
				if err != nil {
					log.Fatalf("Publish error: %s", err)
				}
			}
		}
	}

	nc.Flush()

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	received := 0
	ch := make(chan time.Time, 2)

	mh := func(msg *nats.Msg) {
		received++
		if c.js && c.ack {
			msg.Ack()
		}
		if received == 1 {
			ch <- time.Now()
		}
		if received >= numMsg {
			ch <- time.Now()
		}
	}
	var sub *nats.Subscription

	log.Printf("Starting subscriber, expecting %s messages", humanize.Comma(int64(numMsg)))

	if c.js {
		js, err := nc.JetStream()
		if err != nil {
			log.Fatalf("couldn't create jetstream context: %v", err)
		}

		if c.pull {
			sub, err = js.PullSubscribe(c.subject, JS_PULLCONSUMER_NAME)
			if err != nil {
				println("error PullSubscribe=" + err.Error())
			}
		} else {
			sub, _ = js.Subscribe(c.subject, mh, nats.OrderedConsumer())
			if err != nil {
				log.Fatalf("Push consumer Subscribe error: %v", err)
			}
		}
	} else {
		sub, _ = nc.Subscribe(c.subject, mh)
	}

	sub.SetPendingLimits(-1, -1)
	nc.Flush()
	startwg.Done()

	if c.js && c.pull {
		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.pullBatch <= (numMsg - i) {
					return c.pullBatch
				} else {
					return numMsg - i
				}
			}()
			msgs, err := sub.Fetch(batchSize)
			if err == nil {
				for _, msg := range msgs {
					mh(msg)
				}
				i += len(msgs)
			} else {
				log.Fatalf("pull consumer error: %v", err)
			}
		}
	}

	start := <-ch
	end := <-ch

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	if sub != nil {
		err := sub.Unsubscribe()
		if err != nil {
			log.Printf("error unsubscribing: %v", err)
		}
	}

	nc.Close()
	donewg.Done()
}
