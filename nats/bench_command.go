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
	subject       string
	numPubs       int
	numSubs       int
	numMsg        int
	msgSize       int
	csvFile       string
	noProgress    bool
	request       bool
	reply         bool
	syncPub       bool
	pubBatch      int
	jsTimeout     time.Duration
	js            bool
	storage       string
	streamName    string
	pull          bool
	pullBatch     int
	replicas      int
	purge         bool
	maxAckPending int
}

const (
	JS_PULLCONSUMER_NAME string = "natscli-benchpull"
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
	bench.Flag("no-progress", "Disable progress bar while publishing").Default("false").BoolVar(&c.noProgress)
	bench.Flag("syncpub", "Synchronously publish to the stream").Default("false").BoolVar(&c.syncPub)
	bench.Flag("request", "Request/Reply mode: publishers send requests waits for a reply").Default("false").BoolVar(&c.request)
	bench.Flag("reply", "Request/Reply mode: subscribers send replies").Default("false").BoolVar(&c.reply)
	bench.Flag("js", "Use JetStream streaming").Default("false").BoolVar(&c.js)
	bench.Flag("pubbatch", "Sets the batch size for JS asynchronous publishing").Default("100").IntVar(&c.pubBatch)
	bench.Flag("jstimeout", "Timeout for JS operations").Default("30s").DurationVar(&c.jsTimeout)
	bench.Flag("storage", "JetStream storage (memory/file)").Default("memory").StringVar(&c.storage)
	bench.Flag("stream", "Stream name to use for the benchmark").Default("benchstream").StringVar(&c.streamName)
	bench.Flag("pull", "Uses a JetStream pull consumer rather than an ordered push consumer").Default("false").BoolVar(&c.pull)
	bench.Flag("pullbatch", "Sets the batch size for the JS pull consumer").Default("100").IntVar(&c.pullBatch)
	bench.Flag("replicas", "Number of stream replicas").Default("1").IntVar(&c.replicas)
	bench.Flag("purge", "Purge the stream before running").Default("false").BoolVar(&c.purge)
	bench.Flag("maxackpending", "Max acks pending for JS consumer").Default("-1").IntVar(&c.maxAckPending)

	cheats["bench"] = `# benchmark core nats publish and subscribe with 10 publishers and subscribers
nats bench testsubject --pub 10 --sub 10 --msgs 10000 --size 512

# benchmark core nats request/reply without subscribers using a queue
nats bench testsubject --pub 1 --sub 1 --msgs 10000 --no-queue

# benchmark core nats request/reply with queuing
nats bench testsubject --sub 4 --reply
nats bench testsubject --pub 4 --request --msgs 20000

# benchmark JetStream synchronously acknowledged publishing purging the data first
nats bench testsubject --js --syncpub --pub 10  --msgs 10000 --purge

# benchmark JS publish and push consumers at the same time purging the data first
nats bench testsubject --js --pub 4 --sub 4 --purge

# benchmark JS stream purge and async batched publishing to the stream
nats bench testsubject --js --pub 4 --purge

# benchmark JS stream get replay from the stream using a push consumer
nats bench testsubject --js --sub 4

# benchmark JS stream get replay from the stream using a pull consumer
nats bench testsubject --js --sub 4 --pull

# remember when benchmarking JetStream
Once you are finished benchmarking, remember to free up the resources (i.e. memory and files) consumed by the stream using 'nats stream rm'
`
}

func (c *benchCmd) bench(_ *kingpin.ParseContext) error {
	// first check the sanity of the arguments
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}
	if c.js && c.numSubs > 0 && c.pull {
		log.Print("JetStream pull consumer mode: subscriber(s) will explicitly acknowledge the consumption of messages")
	}
	if c.js && c.numSubs > 0 && !c.pull {
		log.Print("JetStream ordered push consumer mode: subscribers will not acknowledge the consumption of messages")
	}
	if c.numPubs == 0 && c.numSubs == 0 {
		log.Fatal("You must have at least one publisher or at least one subscriber... try adding --pub 1 and/or --sub 1 to the arguments")
	}
	if (c.request || c.reply) && c.js {
		log.Fatal("Request/Reply mode is not applicable to JetStream benchmarking")
	} else if !c.js {
		if c.request || c.reply {
			log.Print("Benchmark in request/reply mode")
			if c.reply {
				// reply mode is open-ended for the number of messages, so don't show the progress bar
				c.noProgress = true
			}
		}
		if c.request && c.reply {
			log.Fatal("Request/Reply mode error: can not be both a requester and a replier at the same time, please use at least two instances of nats bench to benchmark request/reply")
		}
		if c.reply && c.numPubs > 0 && c.numSubs > 0 {
			log.Fatal("Request/Reply mode error: can not have a publisher while in --reply mode")
		}
	}

	if c.js {
		log.Printf("Starting JetStream benchmark [msgs=%s, msgsize=%s, pubs=%d, subs=%d, js=%v, stream=%s  storage=%s, syncpub=%v, pubbatch=%s, jstimeout=%v, pull=%v, pullbatch=%s, maxackpending=%s, replicas=%d, purge=%v]", humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.js, c.streamName, c.storage, c.syncPub, humanize.Comma(int64(c.pubBatch)), c.jsTimeout, c.pull, humanize.Comma(int64(c.pullBatch)), humanize.Comma(int64(c.maxAckPending)), c.replicas, c.purge)
	} else {
		if c.request || c.reply {
			log.Printf("Starting request/reply benchmark [msgs=%s, msgsize=%s, pubs=%d, subs=%d, js=%v, request=%v, reply=%v]", humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.js, c.request, c.reply)
		} else {
			log.Printf("Starting pub/sub benchmark [msgs=%s, msgsize=%s, pubs=%d, subs=%d, js=%v]", humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.js)
		}
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
		}

		js, err = nc.JetStream(nats.MaxWait(c.jsTimeout))
		if err != nil {
			log.Fatalf("Couldn't create jetstream context: %v", err)
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

		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		_, err = js.AddStream(&nats.StreamConfig{Name: c.streamName, Subjects: []string{c.subject}, Retention: nats.LimitsPolicy, Storage: storageType, Replicas: c.replicas})
		if err != nil {
			log.Fatalf("there is already a stream %s defined with conflicting attributes, if you want to delete and re-define the stream use `nats stream delete` (%v)", c.streamName, err)
		}

		if c.purge {
			log.Printf("Purging the stream")
			err = js.PurgeStream(c.streamName)
			if err != nil {
				log.Fatalf("error purging stream %s: %v", c.streamName, err)
			}
		}

		// create the pull consumer
		if c.pull && c.numSubs > 0 {
			_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
				Durable:       JS_PULLCONSUMER_NAME,
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				ReplayPolicy:  nats.ReplayInstantPolicy,
				MaxAckPending: c.maxAckPending,
			})
			if err != nil {
				log.Fatal("error creating the pull consumer: ", err)
			}
			defer func() {
				err := js.DeleteConsumer(c.streamName, JS_PULLCONSUMER_NAME)
				if err != nil {
					log.Printf("error deleting the pull consumer on stream %s: %v", c.streamName, err)
				}
			}()
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
			if c.pull || c.reply {
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
		err := ioutil.WriteFile(c.csvFile, []byte(csv), 0644)
		if err != nil {
			log.Printf("error writing file %s: %v", c.csvFile, err)
		}
		fmt.Printf("Saved metric data in csv file %s\n", c.csvFile)
	}

	return nil
}
func coreNATSPublisher(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int) {

	var m *nats.Msg
	var err error

	errBytes := []byte("error")
	minusByte := byte('-')

	state := "Publishing"

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

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
	state = "Finished  "
}

func jsPublisher(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int) {
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Couldn't create jetstream context: %v", err)
	}

	var state string

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	if !c.syncPub {
		for i := 0; i < numMsg; i += c.pubBatch {
			state = "Publishing"
			for j := 0; j < c.pubBatch && i+j < c.numMsg; j++ {
				_, err = js.PublishAsync(c.subject, msg)
				if err != nil {
					log.Fatalf("PubAsync error: %v", err)
				}
				if progress != nil {
					progress.Incr()
				}
			}

			state = "AckWait   "

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(c.jsTimeout):
				log.Fatalf("JS PubAsync did not receive a positive ack")
			}
		}
		state = "Finished  "
	} else {
		state = "Publishing"
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

func (c *benchCmd) runPublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting publisher, publishing %s messages", humanize.Comma(int64(numMsg)))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	start := time.Now()

	if !c.js {
		coreNATSPublisher(*c, nc, progress, msg, numMsg)
	} else {
		jsPublisher(*c, nc, progress, msg, numMsg)
	}

	err := nc.Flush()
	if err != nil {
		log.Fatalf("error flushing: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	if !c.reply {
		log.Printf("Starting subscriber, expecting %s messages", humanize.Comma(int64(numMsg)))
	} else {
		log.Print("Starting replier, hit control-c to stop")
		c.noProgress = true
	}

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Message handler
	mh := func(msg *nats.Msg) {
		received++
		if c.reply || (c.js && c.pull) {
			err := msg.Ack()
			if err != nil {
				log.Fatalf("error sending a reply message: %v", err)
			}
		}
		if received == 1 {
			ch <- time.Now()
		}
		if received >= numMsg && !c.reply {
			ch <- time.Now()
		}
		if progress != nil {
			progress.Incr()
		}
	}
	var sub *nats.Subscription

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
			state = "Consuming "
			// ordered push consumer
			sub, err = js.Subscribe(c.subject, mh, nats.OrderedConsumer())
			if err != nil {
				log.Fatalf("Push consumer Subscribe error: %v", err)
			}
		}
	} else {
		state = "Receiving "
		if !c.reply {
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

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		log.Fatalf("error setting pending limits on the subscriber: %v", err)
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("error flushing: %v", err)
	}

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

			if progress != nil {
				state = "Pulling   "
			}

			msgs, err := sub.Fetch(batchSize, nats.MaxWait(c.jsTimeout))
			if err == nil {
				if progress != nil {
					state = "Handling  "
				}

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

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}
