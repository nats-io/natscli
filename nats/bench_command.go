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
	noProgress     bool
	request        bool
	reply          bool
	noQueueGroup   bool
	syncPub        bool
	js             bool
	storage        string
	pubBatch       int
	pull           bool
	pullBatch      int
	replicas       int
	noPurge        bool
	noDeleteStream bool
	maxAckPending  int
}

const (
	JS_STREAM_NAME       string = "benchstream"
	JS_PULLCONSUMER_NAME string = "pullconsumer"
)

func configureBenchCommand(app *kingpin.Application) {
	c := &benchCmd{}
	bench := app.Command("bench", "Benchmark utility").Action(c.bench)
	bench.Arg("subject", "Subject to use for testing").Required().StringVar(&c.subject)
	bench.Flag("pub", "Number of concurrent publishers").Default("0").IntVar(&c.numPubs)
	bench.Flag("sub", "Number of concurrent subscribers").Default("0").IntVar(&c.numSubs)
	bench.Flag("msgs", "Number of messages to publish").Default("100000").IntVar(&c.numMsg)
	bench.Flag("size", "Size of the test messages").Default("128").IntVar(&c.msgSize)
	bench.Flag("csv", "Save benchmark data to CSV file").StringVar(&c.csvFile)
	bench.Flag("noprogress", "Disable progress bar while publishing").Default("false").BoolVar(&c.noProgress)
	bench.Flag("syncpub", "Synchronously publish to the stream").Default("false").BoolVar(&c.syncPub)
	bench.Flag("request", "Request/Reply mode: publishers send requests waits for a reply").Default("false").BoolVar(&c.request)
	bench.Flag("reply", "Request/Reply mode: subscribers send replies").Default("false").BoolVar(&c.reply)
	bench.Flag("noqueue", "Request/Reply mode: repliers do not join a queue group ").Default("false").BoolVar(&c.noQueueGroup)
	bench.Flag("js", "Use JetStream streaming").Default("false").BoolVar(&c.js)
	bench.Flag("pubbatch", "Sets the batch size for JS asynchronous publishing").Default("100").IntVar(&c.pubBatch)
	bench.Flag("storage", "JetStream storage (memory/file)").Default("memory").StringVar(&c.storage)
	bench.Flag("pull", "Uses a JetStream pull consumer rather than an ordered push consumer").Default("false").BoolVar(&c.pull)
	bench.Flag("pullbatch", "Sets the batch size for the JS pull consumer").Default("100").IntVar(&c.pullBatch)
	bench.Flag("replicas", "Number of stream replicas").Default("1").IntVar(&c.replicas)
	bench.Flag("nopurge", "Do not purge the stream before running").Default("false").BoolVar(&c.noPurge)
	bench.Flag("nodelete", "Do not delete the stream at the end of the run").Default("false").BoolVar(&c.noDeleteStream)
	bench.Flag("maxackpending", "Max acks pending for JS consumer").Default("-1").IntVar(&c.maxAckPending)

	cheats["bench"] = `# benchmark core nats publish and subscribe with 10 publishers and subscribers
nats bench testsubject --pub 10 --sub 10 --msgs 10000 --size 512

# benchmark core nats request/reply without subscribers using a queue
nats bench testsubject --pub 1 --sub 1 --msgs 10000 --noqueue

# benchmark core nats request/reply with queuing
nats bench testsubject --sub 4 --reply
nats bench testsubject --pub 4 --request --msgs 20000

# benchmark JetStream acknowledged publishes
nats bench testsubject --js --syncpub --pub 10  --msgs 10000


# benchmark JS publish and push consumers at the same time
nats bench testsubject --pub 4 --sub 4 --js

# benchmark JS stream purge and async batched publish into the stream
nats bench testsubject --pub 4 --js --nodelete 

# benchmark JS stream do not purge and get replay from the stream using a push consumer
nats bench testsubject --sub 4 --js --nodelete --nopurge

# benchmark JS stream do not purge and get replay from the stream using a pull consumer
nats bench testsubject --sub 4 --js --nodelete --nopurge --pull
`
}

func (c *benchCmd) bench(_ *kingpin.ParseContext) error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	log.Printf("Starting benchmark [msgs=%s, msgsize=%s, pubs=%d, subs=%d, js=%v, storage=%s, syncpub=%v, pubbatch=%s, pull=%v, pullbatch=%s, request=%v, reply=%v, noqueue=%v, maxackpending=%s, replicas=%d, nopurge=%v, nodelete=%v]", humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.js, c.storage, c.syncPub, humanize.Comma(int64(c.pubBatch)), c.pull, humanize.Comma(int64(c.pullBatch)), c.request, c.reply, c.noQueueGroup, humanize.Comma(int64(c.maxAckPending)), c.replicas, c.noPurge, c.noDeleteStream)

	if c.numPubs == 0 && c.numSubs == 0 {
		log.Fatalf("You must have at least one publisher or at least one subscriber... try adding --pub 1 and/or --sub 1 to the arguments")
	}

	if c.js && c.maxAckPending != -1 && c.maxAckPending < c.numMsg && c.numSubs > 0 {
		log.Printf("WARNING: max acks pending is smaller than the mumber of messages and the subscribers are not set to ack, use --ack or you run the risk of the consumers not receiving all the messages")
	}

	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

	if (c.request || c.reply) && c.js {
		log.Fatalf("Request/Reply mode is not applicable to streams")
	} else if !c.js {
		if c.request || c.reply {
			log.Printf("Benchmark in request/reply mode")
		}
		if c.request && c.reply {
			log.Fatalf("Request/Reply mode error: can not be both a requester and a replier at the same time, please use at least two instances of nats bench to benchmark request/reply")
		}
		if c.reply && c.numPubs > 0 && c.numSubs > 0 {
			log.Fatalf("Request/Reply mode error: can not have a publisher while in --reply mode")
		}
	}

	if c.js {
		// create the stream for the benchmark (and purge it)
		nc, err := nats.Connect(config.ServerURL(), natsOpts()...)
		if err != nil {
			log.Fatalf("NATS connection failed: %s", err)
			return err
		}

		js, err = nc.JetStream()
		if err != nil {
			log.Fatalf("Couldn't create jetstream context: %v", err)
		}

		// First delete any prior stream unless no delete
		if !c.noDeleteStream {
			log.Printf("Deleting any existing stream")
			js.DeleteStream(JS_STREAM_NAME)
		}

		storageType := func() nats.StorageType {
			switch c.storage {
			case "file":
				return nats.FileStorage
			case "memory":
				return nats.MemoryStorage
			default:
				{
					log.Printf("Unknown storage type %s, using memory", c.storage)
					return nats.MemoryStorage
				}
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
			return fmt.Errorf("NATS connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull || (c.reply && !c.noQueueGroup) {
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
			return fmt.Errorf("NATS connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runPublisher(bm, nc, startwg, donewg, pubCounts[i])
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	if !c.noProgress {
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
	if !c.noProgress {
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

		var m *nats.Msg
		var err error

		errBytes := []byte("error")
		minusByte := byte('-')

		for i := 0; i < numMsg; i++ {
			if progress != nil {
				progress.Incr()
			}

			if !c.request {
				err = nc.Publish(c.subject, msg)
				if err != nil {
					log.Fatalf("Publish error: %s", err)
				}
			} else {
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
			log.Fatalf("Couldn't create jetstream context: %v", err)
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
		if c.reply {
			msg.Ack()
		}
		if received == 1 {
			ch <- time.Now()
		}
		if received >= numMsg && !c.reply {
			ch <- time.Now()
		}
	}
	var sub *nats.Subscription

	if !c.reply {
		log.Printf("Starting subscriber, expecting %s messages", humanize.Comma(int64(numMsg)))
	} else {
		log.Printf("Starting replier, hit control-c to stop")
		c.noProgress = true
	}

	var err error

	if c.js {
		var js nats.JetStreamContext

		js, err = nc.JetStream()
		if err != nil {
			log.Fatalf("Couldn't create jetstream context: %v", err)
		}

		if c.pull {
			sub, err = js.PullSubscribe(c.subject, JS_PULLCONSUMER_NAME)
			if err != nil {
				log.Fatalf("Error PullSubscribe=" + err.Error())
			}
		} else {
			// ordered push consumer
			sub, err = js.Subscribe(c.subject, mh, nats.OrderedConsumer())

			if err != nil {
				log.Fatalf("Push consumer Subscribe error: %v", err)
			}
		}
	} else {
		if !c.reply || (c.reply && c.noQueueGroup) {
			sub, err = nc.Subscribe(c.subject, mh)
			if err != nil {
				log.Fatalf("Subscribe error: %v", err)
			}
		} else {
			sub, err = nc.QueueSubscribe(c.subject, "bench-reply", mh)
			if err != nil {
				log.Fatalf("QueueSubscribe error: %v", err)
			}
		}
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
				log.Fatalf("Pull consumer error: %v", err)
			}
		}
	}

	start := <-ch
	end := <-ch

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	nc.Close()
	donewg.Done()
}
