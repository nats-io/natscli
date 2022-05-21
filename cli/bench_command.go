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

package cli

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strconv"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
	"gopkg.in/alecthomas/kingpin.v2"
)

type benchCmd struct {
	subject              string
	numPubs              int
	numSubs              int
	numMsg               int
	msgSizeString        string
	msgSize              int
	csvFile              string
	noProgress           bool
	request              bool
	reply                bool
	syncPub              bool
	pubBatch             int
	jsTimeout            time.Duration
	js                   bool
	storage              string
	streamName           string
	streamMaxBytesString string
	streamMaxBytes       int64
	pull                 bool
	consumerBatch        int
	replicas             int
	purge                bool
	subSleep             time.Duration
	pubSleep             time.Duration
	pushDurable          bool
	consumerName         string
	kv                   bool
	history              uint8
	fetchTimeout         bool
	multiSubject         bool
	multiSubjectMax      int
}

const (
	DEFAULT_DURABLE_CONSUMER_NAME string = "natscli-bench"
	DEFAULT_STREAM_NAME           string = "benchstream"
)

func configureBenchCommand(app commandHost) {
	c := &benchCmd{}
	bench := app.Command("bench", "Benchmark utility").Action(c.bench)
	bench.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	bench.Flag("pub", "Number of concurrent publishers").Default("0").IntVar(&c.numPubs)
	bench.Flag("sub", "Number of concurrent subscribers").Default("0").IntVar(&c.numSubs)
	bench.Flag("js", "Use JetStream").Default("false").BoolVar(&c.js)
	bench.Flag("request", "Request-Reply mode: publishers send requests waits for a reply").Default("false").BoolVar(&c.request)
	bench.Flag("reply", "Request-Reply mode: subscribers send replies").Default("false").BoolVar(&c.reply)
	bench.Flag("kv", "KV mode, subscribers get from the bucket and publishers put in the bucket").Default("false").BoolVar(&c.kv)
	bench.Flag("msgs", "Number of messages to publish").Default("100000").IntVar(&c.numMsg)
	bench.Flag("size", "Size of the test messages").Default("128").StringVar(&c.msgSizeString)
	bench.Flag("no-progress", "Disable progress bar while publishing").Default("false").BoolVar(&c.noProgress)
	bench.Flag("csv", "Save benchmark data to CSV file").StringVar(&c.csvFile)
	bench.Flag("purge", "Purge the stream before running").Default("false").BoolVar(&c.purge)
	bench.Flag("storage", "JetStream storage (memory/file) for the \"benchstream\" stream").Default("memory").EnumVar(&c.storage, "memory", "file")
	bench.Flag("replicas", "Number of stream replicas for the \"benchstream\" stream").Default("1").IntVar(&c.replicas)
	bench.Flag("maxbytes", "The maximum size of the stream or KV bucket in bytes").Default("1GB").StringVar(&c.streamMaxBytesString)
	bench.Flag("stream", "When set to something else than \"benchstream\": use (and do not attempt to define) the specified stream when creating durable subscribers. Otherwise define and use the \"benchstream\" stream").Default(DEFAULT_STREAM_NAME).StringVar(&c.streamName)
	bench.Flag("consumer", "Specify the durable consumer name to use").Default(DEFAULT_DURABLE_CONSUMER_NAME).StringVar(&c.consumerName)
	bench.Flag("jstimeout", "Timeout for JS operations").Default("30s").DurationVar(&c.jsTimeout)
	bench.Flag("syncpub", "Synchronously publish to the stream").Default("false").BoolVar(&c.syncPub)
	bench.Flag("pubbatch", "Sets the batch size for JS asynchronous publishing").Default("100").IntVar(&c.pubBatch)
	bench.Flag("pull", "Use a shared durable explicitly acknowledged JS pull consumer rather than individual ephemeral consumers").Default("false").BoolVar(&c.pull)
	bench.Flag("push", "Use a shared durable explicitly acknowledged JS push consumer with a queue group rather than individual ephemeral consumers").Default("false").BoolVar(&c.pushDurable)
	bench.Flag("consumerbatch", "Sets the batch size for the JS durable pull consumer, or the max ack pending value for the JS durable push consumer").Default("100").IntVar(&c.consumerBatch)
	bench.Flag("pullbatch", "Sets the batch size for the JS durable pull consumer, or the max ack pending value for the JS durable push consumer").Hidden().Default("100").IntVar(&c.consumerBatch)
	bench.Flag("subsleep", "Sleep for the specified interval before sending the subscriber acknowledgement back in --js mode, or sending the reply back in --reply mode,  or doing the next get in --kv mode").Default("0s").DurationVar(&c.subSleep)
	bench.Flag("pubsleep", "Sleep for the specified interval after publishing each message").Default("0s").DurationVar(&c.pubSleep)
	bench.Flag("history", "History depth for the bucket in KV mode").Default("1").Uint8Var(&c.history)
	bench.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").Default("false").BoolVar(&c.multiSubject)
	bench.Flag("multisubjectmax", "The maximum number of subjects to use in multi-subject mode (0 means no max)").Default("0").IntVar(&c.multiSubjectMax)

	cheats["bench"] = `# benchmark core nats publish and subscribe with 10 publishers and subscribers
nats bench testsubject --pub 10 --sub 10 --msgs 10000 --size 512

# benchmark core nats request-reply without subscribers using a queue
nats bench testsubject --pub 1 --sub 1 --msgs 10000 --no-queue

# benchmark core nats request-reply with queuing
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

# simulate a message processing time (for reply mode and pull JS consumers) of 50 microseconds
nats bench testsubject --reply --sub 1 --acksleep 50us

# generate load by publishing messages at an interval of 100 nanoseconds rather than back to back
nats bench testsubject --pub 1 --pubsleep 100ns

# remember when benchmarking JetStream
Once you are finished benchmarking, remember to free up the resources (i.e. memory and files) consumed by the stream using 'nats stream rm'
`
}

func init() {
	registerCommand("bench", 2, configureBenchCommand)
}

func (c *benchCmd) bench(_ *kingpin.ParseContext) error {
	// first check the sanity of the arguments
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}
	msgSize, err := parseStringAsBytes(c.msgSizeString)
	if err != nil || msgSize <= 0 {
		log.Fatal("Can not parse or invalid the value specified for the message size: %s", c.msgSizeString)
	}
	c.msgSize = int(msgSize)
	if c.js && c.numSubs > 0 && c.pull {
		log.Print("JetStream durable pull consumer mode, subscriber(s) will explicitly acknowledge the consumption of messages")
	}
	if c.js && c.numSubs > 0 && c.pushDurable {
		if c.pull {
			log.Fatal("The durable consumer must be either pull or push, it can not be both")
		}
		log.Print("JetStream durable push consumer mode, subscriber(s) will explicitly acknowledge the consumption of messages")
	}
	if c.js && c.numSubs > 0 && !c.pull {
		log.Print("JetStream ephemeral ordered push consumer mode, subscribers will not acknowledge the consumption of messages")
	}
	if c.numPubs == 0 && c.numSubs == 0 {
		log.Fatal("You must have at least one publisher or at least one subscriber... try adding --pub 1 and/or --sub 1 to the arguments")
	}
	if (c.request || c.reply) && c.js {
		log.Fatal("Request-reply mode is not applicable to JetStream benchmarking")
	} else if !c.js && !c.kv {
		if c.request || c.reply {
			log.Print("Benchmark in request-reply mode")
			if c.reply {
				// reply mode is open-ended for the number of messages, so don't show the progress bar
				c.noProgress = true
			}
		}
		if c.request && c.reply {
			log.Fatal("Request-reply mode error: can not be both a requester and a replier at the same time, please use at least two instances of nats bench to benchmark request/reply")
		}
		if c.reply && c.numPubs > 0 && c.numSubs > 0 {
			log.Fatal("Request-reply mode error: can not have a publisher while in --reply mode")
		}
	} else if c.kv {
		if c.js {
			log.Fatal("Can not operate in both --js and --kv mode at the same time")
		}
		log.Print("KV mode, using the subject name as the KV bucket name. Publishers do puts, subscribers do gets")
	}

	if c.js || c.kv {
		size, err := parseStringAsBytes(c.streamMaxBytesString)

		if err != nil || size <= 0 {
			log.Fatalf("Can not parse or invalid the value specified for the max stream/bucket size: %s", c.streamMaxBytesString)
		}
		c.streamMaxBytes = size
	}

	// Print the banner to repeat the arguments being used
	if c.js {
		if c.streamName == DEFAULT_STREAM_NAME {
			log.Printf("Starting JetStream benchmark [subject=%s, multisubject=%v, multisubjectmax=%d, js=%v, msgs=%s, msgsize=%s, pubs=%d, subs=%d, stream=%s, maxbytes=%s, storage=%s, syncpub=%v, pubbatch=%s, jstimeout=%v, pull=%v, consumerbatch=%s, push=%v, consumername=%s, replicas=%d, purge=%v, pubsleep=%v, subsleep=%v]", c.subject, c.multiSubject, c.multiSubjectMax, c.js, humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.streamName, humanize.IBytes(uint64(c.streamMaxBytes)), c.storage, c.syncPub, humanize.Comma(int64(c.pubBatch)), c.jsTimeout, c.pull, humanize.Comma(int64(c.consumerBatch)), c.pushDurable, c.consumerName, c.replicas, c.purge, c.pubSleep, c.subSleep)
		} else {
			log.Printf("Starting JetStream benchmark [subject=%s,  multisubject=%v, multisubjectmax=%d, js=%v, msgs=%s, msgsize=%s, pubs=%d, subs=%d, stream=%s, maxbytes=%s, syncpub=%v, pubbatch=%s, jstimeout=%v, pull=%v, consumerbatch=%s, push=%v, consumername=%s, purge=%v, pubsleep=%v, subsleep=%v]", c.subject, c.multiSubject, c.multiSubjectMax, c.js, humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.streamName, humanize.IBytes(uint64(c.streamMaxBytes)), c.syncPub, humanize.Comma(int64(c.pubBatch)), c.jsTimeout, c.pull, humanize.Comma(int64(c.consumerBatch)), c.pushDurable, c.consumerName, c.purge, c.pubSleep, c.subSleep)
		}
	} else if c.kv {
		log.Printf("Starting KV benchmark [bucket=%s, kv=%v, msgs=%s, msgsize=%s, maxbytes=%s, pubs=%d, sub=%d, storage=%s, replicas=%d, pubsleep=%v, subsleep=%v]", c.subject, c.kv, humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), humanize.IBytes(uint64(c.streamMaxBytes)), c.numPubs, c.numSubs, c.storage, c.replicas, c.pubSleep, c.subSleep)
	} else {
		if c.request || c.reply {
			log.Printf("Starting request-reply benchmark [subject=%s, multisubject=%v, multisubjectmax=%d, request=%v, reply=%v, msgs=%s, msgsize=%s, pubs=%d, subs=%d, pubsleep=%v, subsleep=%v]", c.subject, c.multiSubject, c.multiSubjectMax, c.request, c.reply, humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.pubSleep, c.subSleep)
		} else {
			log.Printf("Starting Core NATS pub/sub benchmark [subject=%s, multisubject=%v, multisubjectmax=%d, msgs=%s, msgsize=%s, pubs=%d, subs=%d, pubsleep=%v, subsleep=%v]", c.subject, c.multiSubject, c.multiSubjectMax, humanize.Comma(int64(c.numMsg)), humanize.IBytes(uint64(c.msgSize)), c.numPubs, c.numSubs, c.pubSleep, c.subSleep)
		}
	}

	bm := bench.NewBenchmark("NATS", c.numSubs, c.numPubs)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	var js nats.JetStreamContext

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

	if c.js || c.kv {
		// create the stream for the benchmark (and purge it)
		nc, err := nats.Connect(opts.Config.ServerURL(), natsOpts()...)
		if err != nil {
			log.Fatalf("NATS connection failed: %v", err)
		}

		js, err = nc.JetStream(nats.MaxWait(c.jsTimeout))
		if err != nil {
			log.Fatalf("Couldn't get the JetStream context: %v", err)
		}
		if c.kv {
			// There is no way to purge all the keys in a KV bucket in a single operation so deleting the bucket instead
			if c.purge {
				err = js.PurgeStream("KV_" + c.subject)
				//err = js.DeleteKeyValue(c.subject)
				if err != nil {
					log.Fatalf("Can not purge the bucket: %v", err)
				}
			}

			// create bucket
			_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: c.subject, History: c.history, Storage: storageType, Description: "nats bench bucket", Replicas: c.replicas, MaxBytes: c.streamMaxBytes})
			if err != nil {
				log.Fatalf("Couldn't create the KV bucket: %v", err)
			}
		} else if c.js {
			if c.streamName == DEFAULT_STREAM_NAME {
				// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
				_, err = js.AddStream(&nats.StreamConfig{Name: c.streamName, Subjects: []string{getSubscribeSubject(c)}, Retention: nats.LimitsPolicy, Discard: nats.DiscardNew, Storage: storageType, Replicas: c.replicas, MaxBytes: c.streamMaxBytes})
				if err != nil {
					log.Fatalf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
				}
			} else if (c.pull || c.pushDurable) && c.numSubs > 0 {
				log.Printf("Using stream: %s", c.streamName)
			}

			if c.purge {
				log.Printf("Purging the stream")
				err = js.PurgeStream(c.streamName)
				if err != nil {
					log.Fatalf("Error purging stream %s: %v", c.streamName, err)
				}
			}

			// create the pull consumer
			if c.numSubs > 0 {
				if c.pull {
					_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
						Durable:       c.consumerName,
						DeliverPolicy: nats.DeliverAllPolicy,
						AckPolicy:     nats.AckExplicitPolicy,
						ReplayPolicy:  nats.ReplayInstantPolicy,
						MaxAckPending: func(a int) int {
							if a >= 10000 {
								return a
							} else {
								return 10000
							}
						}(c.numSubs * c.consumerBatch),
					})
					if err != nil {
						log.Fatalf("Error creating the pull consumer: %v", err)
					}
					defer func() {
						if c.consumerName == DEFAULT_DURABLE_CONSUMER_NAME {
							err := js.DeleteConsumer(c.streamName, c.consumerName)
							if err != nil {
								log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamName, err)
							}
							log.Printf("Deleted durable consumer: %s\n", c.consumerName)
						}
					}()
					log.Printf("Defined durable explicitly acked pull consumer: %s\n", c.consumerName)
				} else if c.pushDurable {
					_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
						Durable:        c.consumerName,
						DeliverSubject: c.consumerName + "-DELIVERY",
						DeliverGroup:   c.consumerName + "-GROUP",
						DeliverPolicy:  nats.DeliverAllPolicy,
						AckPolicy:      nats.AckExplicitPolicy,
						ReplayPolicy:   nats.ReplayInstantPolicy,
						MaxAckPending:  c.consumerBatch * c.numSubs,
					})
					if err != nil {
						log.Fatal("Error creating the durable push consumer: ", err)
					}
					defer func() {
						if c.consumerName == DEFAULT_DURABLE_CONSUMER_NAME {
							err := js.DeleteConsumer(c.streamName, c.consumerName)
							if err != nil {
								log.Fatalf("Error deleting the durable push consumer on stream %s: %v", c.streamName, err)
							}
							log.Printf("Deleted durable consumer: %s\n", c.consumerName)
						}
					}()

					log.Printf("Defined durable explicitly acked push consumer: %s\n", c.consumerName)
				}
			}
		}
	}

	var offset = func(putter int, counts []int) int {
		var position = 0

		for i := 0; i < putter; i++ {
			position = position + counts[i]
		}
		return position
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numSubs)

	for i := 0; i < c.numSubs; i++ {
		nc, err := nats.Connect(opts.Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		numMsg := func() int {
			if c.pull || c.reply || c.pushDurable || c.kv {
				return subCounts[i]
			} else {
				return c.numMsg
			}
		}()

		go c.runSubscriber(bm, nc, startwg, donewg, numMsg, offset(i, subCounts))
	}
	startwg.Wait()

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numPubs)
	trigger := make(chan struct{})
	for i := 0; i < c.numPubs; i++ {
		nc, err := nats.Connect(opts.Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runPublisher(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	if !c.noProgress {
		uiprogress.Stop()
	}

	if c.fetchTimeout {
		log.Print("WARNING: at least one of the pull consumer Fetch operation timed out, the results may not be accurate!")
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getSubscribeSubject(c *benchCmd) string {
	if c.multiSubject {
		return c.subject + ".*"
	} else {
		return c.subject
	}
}

func getPublishSubject(c *benchCmd, number int) string {
	if c.multiSubject {
		if c.multiSubjectMax == 0 {
			return c.subject + "." + strconv.Itoa(number)
		} else {
			return c.subject + "." + strconv.Itoa(number%c.multiSubjectMax)
		}
	} else {
		return c.subject
	}
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
			err = nc.Publish(getPublishSubject(&c, i), msg)
			if err != nil {
				log.Fatalf("Publish error: %v", err)
			}
		} else {
			m, err = nc.Request(getPublishSubject(&c, i), msg, time.Second)
			if err != nil {
				log.Fatalf("Request error %v", err)
			}

			if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
				log.Fatalf("Publish Request did not receive a positive ACK: %q", m.Data)
			}
		}
		time.Sleep(c.pubSleep)
	}
	state = "Finished  "
}

func jsPublisher(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int) {
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
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
			futures := make([]nats.PubAckFuture, min(c.pubBatch, c.numMsg-i))
			for j := 0; j < c.pubBatch && i+j < c.numMsg; j++ {
				futures[j], err = js.PublishAsync(getPublishSubject(&c, i), msg)
				if err != nil {
					log.Fatalf("PubAsync error: %v", err)
				}
				if progress != nil {
					progress.Incr()
				}
				time.Sleep(c.pubSleep)
			}

			state = "AckWait   "

			select {
			case <-js.PublishAsyncComplete():
				state = "ProcessAck"
				for future := range futures {
					select {
					case <-futures[future].Ok():
					case e := <-futures[future].Err():
						log.Fatalf("PubAsyncFuture for message %v in batch not OK: %v", future, e)
					}
				}
			case <-time.After(c.jsTimeout):
				log.Fatalf("JS PubAsync did not receive an ack/error")
			}
		}
		state = "Finished  "
	} else {
		state = "Publishing"
		for i := 0; i < numMsg; i++ {
			if progress != nil {
				progress.Incr()
			}
			_, err = js.Publish(getPublishSubject(&c, i), msg)
			if err != nil {
				log.Fatalf("Publish: %v", err)
			}
			time.Sleep(c.pubSleep)
		}
	}
}

func kvPutter(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) {
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	kvBucket, err := js.KeyValue(c.subject)
	if err != nil {
		log.Fatalf("Couldn't find kv store %s: %v", c.subject, err)
	}

	var state string = "Putting   "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}
		_, err = kvBucket.Put(fmt.Sprintf("%d", offset+i), msg)
		if err != nil {
			log.Fatalf("Put: %s", err)
		}
		time.Sleep(c.pubSleep)
	}
}

func (c *benchCmd) runPublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int) {
	startwg.Done()

	var progress *uiprogress.Bar

	if c.kv {
		log.Printf("Starting KV putter, putting %s messages", humanize.Comma(int64(numMsg)))
	} else {
		log.Printf("Starting publisher, publishing %s messages", humanize.Comma(int64(numMsg)))
	}

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	start := time.Now()

	if !c.js && !c.kv {
		coreNATSPublisher(*c, nc, progress, msg, numMsg)
	} else if c.kv {
		kvPutter(*c, nc, progress, msg, numMsg, offset)
	} else if c.js {
		jsPublisher(*c, nc, progress, msg, numMsg)
	}

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int) {
	received := 0
	fetchTimeout := false

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	if !c.reply {
		if c.kv {
			log.Printf("Starting KV getter, trying to get %s messages", humanize.Comma(int64(numMsg)))
		} else {
			log.Printf("Starting subscriber, expecting %s messages", humanize.Comma(int64(numMsg)))
		}
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
		if c.reply || (c.js && (c.pull || c.pushDurable)) {
			time.Sleep(c.subSleep)
			err := msg.Ack()
			if err != nil {
				log.Fatalf("Error sending a reply message: %v", err)
			}
		}

		if !c.js && received == 1 {
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

	if !c.kv {
		// create the subscriber
		if c.js {
			var js nats.JetStreamContext

			js, err = nc.JetStream()
			if err != nil {
				log.Fatalf("Couldn't get the JetStream context: %v", err)
			}
			// start the timer now rather than when the first message is received in JS mode

			startTime := time.Now()
			ch <- startTime
			if progress != nil {
				progress.TimeStarted = startTime
			}
			if c.pull {
				sub, err = js.PullSubscribe("", c.consumerName, nats.BindStream(c.streamName))
				if err != nil {
					log.Fatalf("Error PullSubscribe: %v", err)
				}
			} else if c.pushDurable {
				state = "Receiving "
				sub, err = js.QueueSubscribe(getSubscribeSubject(c), c.consumerName+"-GROUP", mh, nats.Bind(c.streamName, c.consumerName), nats.ManualAck())
				if err != nil {
					log.Fatalf("Error push durable Subscribe: %v", err)
				}
				_ = sub.AutoUnsubscribe(numMsg)

			} else {
				state = "Consuming "
				// ordered push consumer
				sub, err = js.Subscribe(getSubscribeSubject(c), mh, nats.OrderedConsumer())
				if err != nil {
					log.Fatalf("Push consumer Subscribe error: %v", err)
				}
			}
		} else {
			state = "Receiving "
			if !c.reply {
				sub, err = nc.Subscribe(getSubscribeSubject(c), mh)
				if err != nil {
					log.Fatalf("Subscribe error: %v", err)
				}
			} else {
				sub, err = nc.QueueSubscribe(getSubscribeSubject(c), "bench-reply", mh)
				if err != nil {
					log.Fatalf("QueueSubscribe error: %v", err)
				}
			}
		}

		err = sub.SetPendingLimits(-1, -1)
		if err != nil {
			log.Fatalf("Error setting pending limits on the subscriber: %v", err)
		}
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	if c.kv {
		var js nats.JetStreamContext

		js, err = nc.JetStream()
		if err != nil {
			log.Fatalf("Couldn't get the JetStream context: %v", err)
		}

		kvBucket, err := js.KeyValue(c.subject)
		if err != nil {
			log.Fatalf("Couldn't find kv store %s: %v", c.subject, err)
		}

		// start the timer now rather than when the first message is received in JS mode
		startTime := time.Now()
		ch <- startTime
		if progress != nil {
			progress.TimeStarted = startTime
		}

		state = "Getting   "
		for i := 0; i < numMsg; i++ {
			entry, err := kvBucket.Get(fmt.Sprintf("%d", offset+i))
			if err != nil {
				log.Fatalf("Error getting key %d: %v", offset+i, err)
			}
			if entry.Value() == nil {
				log.Printf("Warning: got no value for key %d", offset+i)
			}

			if progress != nil {
				progress.Incr()
			}
			time.Sleep(c.subSleep)
		}
		ch <- time.Now()
	} else if c.js && c.pull {
		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.consumerBatch <= (numMsg - i) {
					return c.consumerBatch
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
				if err == nats.ErrTimeout {
					if c.noProgress {
						log.Print("Fetch timeout!")
					}
					fetchTimeout = true
				} else {
					log.Fatalf("Pull consumer Fetch error: %v", err)
				}
			}
		}
	}

	start := <-ch
	end := <-ch

	if !c.kv {
		_ = sub.Drain()
	}

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	if fetchTimeout {
		c.fetchTimeout = true
	}

	donewg.Done()
}
