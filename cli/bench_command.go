// Copyright 2020-2024 The NATS Authors
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
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/gosuri/uiprogress"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/bench"
	services "github.com/nats-io/nats.go/micro"
)

type benchCmd struct {
	subject              string
	numClients           int
	numMsg               int
	msgSizeString        string
	msgSize              int
	csvFile              string
	noProgress           bool
	jsTimeout            time.Duration
	storage              string
	streamName           string
	createStream         bool
	streamMaxBytesString string
	streamMaxBytes       int64
	ackMode              string
	batchSize            int
	replicas             int
	purge                bool
	sleep                time.Duration
	consumerName         string
	bucketName           string
	history              uint8
	fetchTimeout         bool
	multiSubject         bool
	multiSubjectMax      int
	deDuplication        bool
	deDuplicationWindow  time.Duration
	retries              int
	retriesUsed          bool
	ack                  bool
}

const (
	DefaultDurableConsumerName string = "nats-bench"
	DefaultStreamName          string = "benchstream"
	DefaultBucketName          string = "benchbucket"
	DefaultServiceName         string = "nats-bench-service"
	DefaultServiceVersion      string = "1.0.0"
	BenchTypeCorePub           string = "pub"
	BenchTypeCoreSub           string = "sub"
	BenchTypeServiceRequest    string = "request"
	BenchTypeServiceServe      string = "reply"
	BenchTypeJSPub             string = "jspub"
	BenchTypeJSOrdered         string = "jsordered"
	BenchTypeJSConsume         string = "jsconsume"
	BenchTypeJSFetch           string = "jsfetch"
	BenchTypeOldJSOrdered      string = "oldjsordered"
	BenchTypeOldJSPush         string = "oldjspush"
	BenchTypeOldJSPull         string = "oldjspull"
	BenchTypeKVPut             string = "kvput"
	BenchTypeKVGet             string = "kvget"
	AckModeNone                string = "none"
	AckModeAll                 string = "all"
	AckModeExplicit            string = "explicit"
)

func configureBenchCommand(app commandHost) {
	c := &benchCmd{}

	benchHelp := `
Core NATS publish:

  nats bench pub benchsubject

Core NATS subscribe:

  nats bench sub benchsubject

Service request reply:

  nats bench service serve benchsubject
  nats bench service request benchsubject

JetStream publish, creating the stream if it doesn't exist:

  nats bench js pub benchsubject --create

JetStream ordered ephemeral consumer using the Consume() callback:

  nats bench js ordered

JetStream durable consumer (acks none) using the Consume() callback:

  nats bench js consume --acks=none

JetStream durable consumer (acks explicit) fetching 1000 at a time:

  nats bench js fetch --batch=1000

JetStream KV put and get (10 clients):

  nats bench kv put --clients=10
  nats bench kv get --clients=10

Adjust the number of clients and messages with --clients and --msgs.
Set --batch=1 to make the JS publish synchronous. Throttle publishing and emulate consumer processing time with --sleep.

The default names: stream name is 'benchstream' and the durable consumer name is 'nats-bench'.
You can also use the old JetStream API with the oldjs commands: oldjs ordered, oldjs push, oldjs pull.

Remember to use --no-progress to measure performance more accurately.
`

	addCommonFlags := func(f *fisk.CmdClause) {
		f.Flag("clients", "Number of concurrent clients").Default("1").IntVar(&c.numClients)
		f.Flag("msgs", "Number of messages to publish or subscribe to").Default("100000").IntVar(&c.numMsg)
		f.Flag("no-progress", "Disable progress bar").UnNegatableBoolVar(&c.noProgress)
		f.Flag("csv", "Save benchmark data to CSV file").StringVar(&c.csvFile)
		f.Flag("size", "Size of the test messages").Default("128").StringVar(&c.msgSizeString)
		// TODO: support randomized payload data
	}

	addPubFlags := func(f *fisk.CmdClause) {
		f.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)
		f.Flag("multisubjectmax", "The maximum number of subjects to use in multi-subject mode (0 means no max)").Default("100000").IntVar(&c.multiSubjectMax)
		f.Flag("sleep", "Sleep for the specified interval after publishing each message").Default("0s").DurationVar(&c.sleep)
	}

	addJSCommonFlags := func(f *fisk.CmdClause) {
		f.Flag("stream", "The name of the stream to create or use").Default(DefaultStreamName).StringVar(&c.streamName)
		f.Flag("js-timeout", "Timeout for JS operations").Default("30s").DurationVar(&c.jsTimeout)
	}

	addJSPubFlags := func(f *fisk.CmdClause) {
		f.Flag("create", "Create or update the stream first").UnNegatableBoolVar(&c.createStream)
		f.Flag("purge", "Purge the stream before running").UnNegatableBoolVar(&c.purge)
		f.Flag("storage", "JetStream storage (memory/file) for the \"benchstream\" stream").Default("file").EnumVar(&c.storage, "memory", "file")
		f.Flag("replicas", "Number of stream replicas for the \"benchstream\" stream").Default("1").IntVar(&c.replicas)
		f.Flag("maxbytes", "The maximum size of the stream or KV bucket in bytes").Default("1GB").StringVar(&c.streamMaxBytesString)
		f.Flag("retries", "The maximum number of retries in JS operations").Default("3").IntVar(&c.retries)
		f.Flag("dedup", "Sets a message id in the header to use JS Publish de-duplication").Default("false").UnNegatableBoolVar(&c.deDuplication)
		f.Flag("dedupwindow", "Sets the duration of the stream's deduplication functionality").Default("2m").DurationVar(&c.deDuplicationWindow)
	}

	addKVFlags := func(f *fisk.CmdClause) {
		f.Flag("purge", "Purge the stream before running").UnNegatableBoolVar(&c.purge)
		f.Flag("storage", "JetStream storage (memory/file) for the \"benchstream\" stream").Default("file").EnumVar(&c.storage, "memory", "file")
		f.Flag("replicas", "Number of stream replicas for the \"benchstream\" stream").Default("1").IntVar(&c.replicas)
		f.Flag("maxbytes", "The maximum size of the stream or KV bucket in bytes").Default("1GB").StringVar(&c.streamMaxBytesString)
		f.Flag("retries", "The maximum number of retries in JS operations").Default("3").IntVar(&c.retries)
		f.Flag("history", "History depth for the bucket in KV mode").Default("1").Uint8Var(&c.history)
	}

	benchCommand := app.Command("bench", "Benchmark utility")
	addCheat("bench", benchCommand)

	benchCommand.HelpLong(benchHelp)

	corePub := benchCommand.Command("pub", "Publish Core NATS messages").Action(c.pubAction)
	corePub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	addCommonFlags(corePub)
	addPubFlags(corePub)

	coreSub := benchCommand.Command("sub", "Subscribe to Core NATS messages").Action(c.subAction)
	coreSub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	coreSub.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)
	addCommonFlags(coreSub)

	microService := benchCommand.Command("service", "Micro-service mode")

	request := microService.Command("request", "Send requests and wait for a reply").Action(c.requestAction)
	request.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	// TODO: support randomized payload data
	request.Flag("sleep", "Sleep for the specified interval between requests").Default("0s").DurationVar(&c.sleep)
	addCommonFlags(request)

	reply := microService.Command("serve", "Serve requests").Action(c.serveAction)
	reply.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	reply.Flag("sleep", "Sleep for the specified interval before replying to the request").Default("0s").DurationVar(&c.sleep)
	addCommonFlags(reply)

	jsCommand := benchCommand.Command("js", "JetStream benchmark commands")

	jspub := jsCommand.Command("pub", "Publish JetStream messages").Action(c.jspubAction)
	jspub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	jspub.Flag("batch", "The number of asynchronous JS publish calls before waiting for all the publish acknowledgements (set to 1 for synchronous)").Default("100").IntVar(&c.batchSize)
	addCommonFlags(jspub)
	addPubFlags(jspub)
	addJSCommonFlags(jspub)
	addJSPubFlags(jspub)

	jsOrdered := jsCommand.Command("ordered", "Consume JetStream messages from a consumer using an ephemeral ordered consumer").Action(c.jsOrderedAction)
	jsOrdered.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	jsOrdered.Flag("sleep", "Sleep for the specified interval before returning from the message handler").Default("0s").DurationVar(&c.sleep)
	addCommonFlags(jsOrdered)
	addJSCommonFlags(jsOrdered)

	jsConsume := jsCommand.Command("consume", "Consume JetStream messages from a durable consumer using a callback").Action(c.jsConsumeAction)
	jsConsume.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	jsConsume.Flag("sleep", "Sleep for the specified interval before acknowledging the message").Default("0s").DurationVar(&c.sleep)
	jsConsume.Flag("batch", "Sets the max number of messages that can be buffered in the client").Default("500").IntVar(&c.batchSize)
	jsConsume.Flag("ack", "Acknowledgement mode for the consumer").Default(AckModeExplicit).EnumVar(&c.ackMode, AckModeExplicit, AckModeNone, AckModeAll)
	addCommonFlags(jsConsume)
	addJSCommonFlags(jsConsume)

	jsFetch := jsCommand.Command("fetch", "Consume JetStream messages from a durable consumer using fetch").Action(c.jsFetchAction)
	jsFetch.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	jsFetch.Flag("sleep", "Sleep for the specified interval before acknowledging the message").Default("0s").DurationVar(&c.sleep)
	jsFetch.Flag("batch", "Sets the fetch batch size").Default("500").IntVar(&c.batchSize)
	jsFetch.Flag("ack", "Acknowledgement mode for the consumer").Default(AckModeExplicit).EnumVar(&c.ackMode, AckModeExplicit, AckModeNone, AckModeAll)
	addCommonFlags(jsFetch)
	addJSCommonFlags(jsFetch)

	kvCommand := benchCommand.Command("kv", "KV benchmark operations")

	kvput := kvCommand.Command("put", "Put messages in a KV bucket").Action(c.kvPutAction)
	kvput.Arg("bucket", "The bucket to use for the benchmark").Default(DefaultBucketName).StringVar(&c.bucketName)
	kvput.Flag("sleep", "Sleep for the specified interval after putting each message").Default("0s").DurationVar(&c.sleep)
	// TODO: support randomized payload data
	addCommonFlags(kvput)
	addJSCommonFlags(kvput)
	addKVFlags(kvput)

	kvget := kvCommand.Command("get", "Get messages from a KV bucket").Action(c.kvGetAction)
	kvget.Arg("bucket", "The bucket to use for the benchmark").Default(DefaultBucketName).StringVar(&c.bucketName)
	kvget.Flag("sleep", "Sleep for the specified interval after getting each message").Default("0s").DurationVar(&c.sleep)
	addCommonFlags(kvget)
	addJSCommonFlags(kvget)

	oldJSCommand := benchCommand.Command("oldjs", "JetStream benchmark commands using the old JS API")

	oldJSOrdered := oldJSCommand.Command("ordered", "Consume JetStream messages from a consumer using an old JS API's ephemeral ordered consumer").Action(c.oldjsOrderedAction)
	oldJSOrdered.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	oldJSOrdered.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)
	oldJSOrdered.Flag("sleep", "Sleep for the specified interval before returning from the message handler").Default("0s").DurationVar(&c.sleep)
	addCommonFlags(oldJSOrdered)
	addJSCommonFlags(oldJSOrdered)

	oldJSPush := oldJSCommand.Command("push", "Consume JetStream messages from a consumer using an old JS API's durable push consumer").Action(c.oldjsPushAction)
	oldJSPush.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	oldJSPush.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	oldJSPush.Flag("sleep", "Sleep for the specified interval before acknowledging the message").Default("0s").DurationVar(&c.sleep)
	oldJSPush.Flag("maxacks", "Sets the max ack pending value, adjusts for the number of clients").Default("500").IntVar(&c.batchSize)
	oldJSPush.Flag("ack", "Uses explicit message acknowledgement or not for the consumer").Default("true").BoolVar(&c.ack)
	addCommonFlags(oldJSPush)
	addJSCommonFlags(oldJSPush)

	oldJSPull := oldJSCommand.Command("pull", "Consume JetStream messages from a consumer using an old JS API's durable pull consumer").Action(c.oldjsPullAction)
	oldJSPull.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	oldJSPull.Flag("consumer", "Specify the durable consumer name to use").Default(DefaultDurableConsumerName).StringVar(&c.consumerName)
	oldJSPull.Flag("sleep", "Sleep for the specified interval before acknowledging the message").Default("0s").DurationVar(&c.sleep)
	oldJSPull.Flag("batch", "Sets the fetch size for the consumer").Default("500").IntVar(&c.batchSize)
	oldJSPull.Flag("ack", "Uses explicit message acknowledgement or not for the consumer").Default("true").BoolVar(&c.ack)
	addCommonFlags(oldJSPull)
	addJSCommonFlags(oldJSPull)
}

func init() {
	registerCommand("bench", 2, configureBenchCommand)
}

func (c *benchCmd) processActionArgs() error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	// for pubs/request/and put only
	if c.msgSizeString != "" {
		msgSize, err := parseStringAsBytes(c.msgSizeString)
		if err != nil || msgSize <= 0 || msgSize > math.MaxInt {
			return fmt.Errorf("can not parse or invalid the value specified for the message size: %s", c.msgSizeString)
		} else {
			c.msgSize = int(msgSize)
		}
	}

	if opts().Config == nil {
		log.Fatalf("Unknown context %q", opts().CfgCtx)
	}

	if c.streamMaxBytesString != "" {
		size, err := parseStringAsBytes(c.streamMaxBytesString)
		if err != nil || size <= 0 {
			return fmt.Errorf("can not parse or invalid the value specified for the max stream/bucket size: %s", c.streamMaxBytesString)
		}

		c.streamMaxBytes = size
	}

	return nil
}

func (c *benchCmd) generateBanner(benchType string) string {
	// Create the banner which includes the appropriate argument names and values for the type of benchmark being run
	type nvp struct {
		name  string
		value string
	}

	var argnvps []nvp

	streamOrBucketAttribues := func() {
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		if c.createStream {
			argnvps = append(argnvps, nvp{"storage", c.storage})
			argnvps = append(argnvps, nvp{"max-bytes", f(uint64(c.streamMaxBytes))})
			argnvps = append(argnvps, nvp{"replicas", f(c.replicas)})
			argnvps = append(argnvps, nvp{"deduplication", f(c.deDuplication)})
			argnvps = append(argnvps, nvp{"dedup-window", f(c.deDuplicationWindow)})
		}
	}

	benchTypeLabel := "Unknown benchmark"

	switch benchType {
	case BenchTypeCorePub:
		benchTypeLabel = "Core NATS publish"
		argnvps = append(argnvps, nvp{"subject", getSubscribeSubject(c)})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case BenchTypeCoreSub:
		benchTypeLabel = "Core NATS subscribe"
		argnvps = append(argnvps, nvp{"subject", getSubscribeSubject(c)})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
	case BenchTypeServiceRequest:
		benchTypeLabel = "Core NATS service request"
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case BenchTypeServiceServe:
		benchTypeLabel = "Core NATS service serve"
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case BenchTypeJSPub:
		benchTypeLabel = "JetStream publish"
		argnvps = append(argnvps, nvp{"subject", getSubscribeSubject(c)})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		streamOrBucketAttribues()
	case BenchTypeJSOrdered:
		benchTypeLabel = "JetStream ordered ephemeral consumer"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		streamOrBucketAttribues()
	case BenchTypeJSConsume:
		benchTypeLabel = "JetStream durable consumer (callback)"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"ack", c.ackMode})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		streamOrBucketAttribues()
	case BenchTypeJSFetch:
		benchTypeLabel = "JetStream durable consumer (fetch)"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"ack", c.ackMode})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		streamOrBucketAttribues()
	case BenchTypeOldJSOrdered:
		benchTypeLabel = "old JetStream API ordered ephemeral consumer"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		streamOrBucketAttribues()
	case BenchTypeOldJSPush:
		benchTypeLabel = "old JetStream API durable push consumer"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"acked", fmt.Sprintf("%v", c.ack)})
		streamOrBucketAttribues()
	case BenchTypeOldJSPull:
		benchTypeLabel = "old JetStream API durable pull consumer"
		argnvps = append(argnvps, nvp{"stream", c.streamName})
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"acked", fmt.Sprintf("%v", c.ack)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		streamOrBucketAttribues()
	case BenchTypeKVPut:
		benchTypeLabel = "KV put"
		argnvps = append(argnvps, nvp{"bucket", c.bucketName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		streamOrBucketAttribues()
	case BenchTypeKVGet:
		benchTypeLabel = "KV get"
		argnvps = append(argnvps, nvp{"bucket", c.bucketName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		streamOrBucketAttribues()
	}

	argnvps = append(argnvps, nvp{"msgs", f(c.numMsg)})
	argnvps = append(argnvps, nvp{"msg-size", humanize.IBytes(uint64(c.msgSize))})
	argnvps = append(argnvps, nvp{"clients", f(c.numClients)})

	banner := fmt.Sprintf("Starting %s benchmark [", benchTypeLabel)

	var joinBuffer []string

	for _, v := range argnvps {
		joinBuffer = append(joinBuffer, v.name+"="+v.value)
	}
	banner += strings.Join(joinBuffer, ", ") + "]"

	return banner
}

func (c *benchCmd) printResults(bm *bench.Benchmark) error {
	if !c.noProgress {
		uiprogress.Stop()
	}

	if c.fetchTimeout {
		log.Print("WARNING: at least one of the pull consumer Fetch operation timed out. These results are not optimal!")
	}

	if c.retriesUsed {
		log.Print("WARNING: at least one of the JS publish operations had to be retried. These results are not optimal!")
	}

	fmt.Println()
	fmt.Println(bm.Report())

	if c.csvFile != "" {
		csv := bm.CSV()
		err := os.WriteFile(c.csvFile, []byte(csv), 0600)
		if err != nil {
			return fmt.Errorf("error writing file %s: %v", c.csvFile, err)
		}
		fmt.Printf("Saved metric data in csv file %s\n", c.csvFile)
	}

	return nil
}

func offset(putter int, counts []int) int {
	var position = 0

	for i := 0; i < putter; i++ {
		position = position + counts[i]
	}
	return position
}

func (c *benchCmd) storageType() jetstream.StorageType {
	switch c.storage {
	case "file":
		return jetstream.FileStorage
	case "memory":
		return jetstream.MemoryStorage
	default:
		{
			log.Printf("Unknown storage type %s, using memory", c.storage)
			return jetstream.MemoryStorage
		}
	}
}

func getJS(nc *nats.Conn) (jetstream.JetStream, error) {
	var err error
	var js jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js, err = jetstream.New(nc)
	}
	if err != nil {
		return nil, fmt.Errorf("couldn't get the new API JetStream instance: %v", err)
	}

	return js, nil
}

func (c *benchCmd) createOrUpdateConsumer(js jetstream.JetStream) error {
	var ack jetstream.AckPolicy

	switch c.ackMode {
	case AckModeNone:
		ack = jetstream.AckNonePolicy
	case AckModeAll:
		ack = jetstream.AckAllPolicy
	case AckModeExplicit:
		ack = jetstream.AckExplicitPolicy
	}

	_, err := js.CreateOrUpdateConsumer(ctx, c.streamName, jetstream.ConsumerConfig{
		Durable:           c.consumerName,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         ack,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		MaxAckPending:     c.batchSize * c.numClients,
		InactiveThreshold: time.Second * 10,
	})
	if err != nil {
		return fmt.Errorf("could not create the durable consumer %s: %v", c.consumerName, err)
	}

	return nil
}

// Actions for the various bench commands below
func (c *benchCmd) pubAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeCorePub)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCorePublisher(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts), strconv.Itoa(i))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) subAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeCoreSub)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreSubscriber(bm, nc, startwg, donewg, c.numMsg)
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) requestAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeServiceRequest)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreRequester(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts), strconv.Itoa(i))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) serveAction(_ *fisk.ParseContext) error {
	// reply mode is open-ended for the number of messages
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeServiceServe)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runServiceServer(nc, startwg, donewg)
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jspubAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeJSPub)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", 0, c.numClients)
	benchId := strconv.FormatInt(time.Now().UnixMilli(), 16)
	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	// create the stream for the benchmark (and purge it)
	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	var js2 jetstream.JetStream

	switch {
	case opts().JsDomain != "":
		js2, err = jetstream.NewWithDomain(nc, opts().JsDomain)
	case opts().JsApiPrefix != "":
		js2, err = jetstream.NewWithAPIPrefix(nc, opts().JsApiPrefix)
	default:
		js2, err = jetstream.New(nc)
	}
	if err != nil {
		log.Fatalf("Couldn't get the new API JetStream instance: %v", err)
	}

	var s jetstream.Stream
	if c.createStream {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		s, err = js2.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: c.streamName, Subjects: []string{getSubscribeSubject(c)}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: c.storageType(), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
		if err != nil {
			log.Fatalf("%v. If you want to delete and re-define the stream use `nats stream delete %s`.", err, c.streamName)
		}
		// TODO: a way to wait for the stream to be ready (e.g. when updating the stream's config (e.g. from R1 to R3))
	} else {
		s, err = js2.Stream(ctx, c.streamName)
		if err != nil {
			log.Fatalf("Stream %s does not exist, can create it with --create: %v", c.streamName, err)
		}
		log.Printf("Using stream: %s", c.streamName)
	}

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			log.Fatalf("Error purging stream %s: %v", c.streamName, err)
		}
	}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSPublisher(bm, nc, startwg, donewg, trigger, pubCounts[i], offset(i, pubCounts), benchId, strconv.Itoa(i))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsOrderedAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeJSOrdered)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("could not establish NATS connection for client number %d: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, nc, startwg, donewg, BenchTypeJSOrdered, c.numMsg)
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsConsumeAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeJSConsume)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("could not establish NATS connection: %v", err)
	}
	defer nc.Close()

	js, err := getJS(nc)
	if err != nil {
		return err
	}

	if c.consumerName == DefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it just delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}

		defer func() {
			err := js.DeleteConsumer(ctx, c.streamName, c.consumerName)
			if err != nil {
				log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("could not establish NATS connection for client number %d: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, nc, startwg, donewg, BenchTypeJSConsume, subCounts[i])
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) jsFetchAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeJSFetch)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("could not establish NATS connection: %v", err)
	}
	defer nc.Close()

	js, err := getJS(nc)
	if err != nil {
		return err
	}

	if c.consumerName == DefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it be just create or delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}

		defer func() {
			err := js.DeleteConsumer(ctx, c.streamName, c.consumerName)
			if err != nil {
				log.Fatalf("Error deleting the consumer on stream %s: %v", c.streamName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("could not establish NATS connection for client number %d: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, nc, startwg, donewg, BenchTypeJSFetch, subCounts[i])
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) kvPutAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeKVPut)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("could not establish NATS connection : %v", err)
	}
	defer nc.Close()

	js, err := getJS(nc)
	if err != nil {
		return err
	}

	// There is no way to purge all the keys in a KV bucket in a single operation so deleting the bucket instead
	if c.purge {
		err = js.DeleteStream(ctx, "KV_"+c.bucketName)
		// err = js.DeleteKeyValue(c.subject)
		if err != nil {
			log.Fatalf("Error trying to purge the bucket: %v", err)
		}
	}

	if c.bucketName == DefaultBucketName {
		// create bucket
		_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: c.bucketName, History: c.history, Storage: c.storageType(), Description: "nats bench bucket", Replicas: c.replicas, MaxBytes: c.streamMaxBytes})
		if err != nil {
			log.Fatalf("Couldn't create the KV bucket: %v", err)
		}
	}

	startwg.Wait()

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVPutter(bm, nc, startwg, donewg, trigger, pubCounts[i], strconv.Itoa(i), offset(i, pubCounts))
	}

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) kvGetAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeKVGet)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVGetter(bm, nc, startwg, donewg, subCounts[i], offset(i, subCounts))
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsOrderedAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeOldJSOrdered)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, nc, startwg, donewg, c.numMsg, BenchTypeOldJSOrdered)
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsPushAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeOldJSPush)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}

	js, err := nc.JetStream(append(jsOpts(), nats.MaxWait(c.jsTimeout))...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	if c.consumerName == DefaultDurableConsumerName {
		ack := nats.AckNonePolicy
		if c.ack {
			ack = nats.AckExplicitPolicy
		}
		maxAckPending := 0
		if c.ack {
			maxAckPending = c.batchSize * c.numClients
		}
		_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
			Durable:        c.consumerName,
			DeliverSubject: c.consumerName + "-DELIVERY",
			DeliverGroup:   c.consumerName + "-GROUP",
			DeliverPolicy:  nats.DeliverAllPolicy,
			AckPolicy:      ack,
			ReplayPolicy:   nats.ReplayInstantPolicy,
			MaxAckPending:  maxAckPending,
		})
		if err != nil {
			log.Fatal("Error creating the durable push consumer: ", err)
		}

		defer func() {
			err := js.DeleteConsumer(c.streamName, c.consumerName)
			if err != nil {
				log.Fatalf("Error deleting the durable push consumer on stream %s: %v", c.streamName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
	}

	if c.ack {
		log.Printf("Defined durable explicitly acked push consumer: %s\n", c.consumerName)
	} else {
		log.Printf("Defined durable unacked push consumer: %s\n", c.consumerName)
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, nc, startwg, donewg, subCounts[i], BenchTypeOldJSPush)
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c *benchCmd) oldjsPullAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(BenchTypeOldJSPull)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		log.Fatalf("NATS connection failed: %v", err)
	}

	js, err := nc.JetStream(append(jsOpts(), nats.MaxWait(c.jsTimeout))...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	ack := nats.AckNonePolicy
	if c.ack {
		ack = nats.AckExplicitPolicy
	}

	if c.consumerName == DefaultDurableConsumerName {
		_, err = js.AddConsumer(c.streamName, &nats.ConsumerConfig{
			Durable:       c.consumerName,
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     ack,
			ReplayPolicy:  nats.ReplayInstantPolicy,
			MaxAckPending: func(a int) int {
				if a >= 10000 {
					return a
				} else {
					return 10000
				}
			}(c.numClients * c.batchSize),
		})
		if err != nil {
			log.Fatalf("Error creating the pull consumer: %v", err)
		}
		defer func() {
			err := js.DeleteConsumer(c.streamName, c.consumerName)
			if err != nil {
				log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
		log.Printf("Defined durable pull consumer: %s\n", c.consumerName)
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("nats connection %d failed: %s", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, nc, startwg, donewg, subCounts[i], BenchTypeOldJSPull)
	}
	startwg.Wait()

	if !c.noProgress {
		uiprogress.Start()
	}

	donewg.Wait()

	bm.Close()

	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
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

func coreNATSPublisher(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) {
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

		err := nc.Publish(getPublishSubject(&c, i+offset), msg)
		if err != nil {
			log.Fatalf("Publish error: %v", err)
		}

		time.Sleep(c.sleep)
	}

	state = "Finished  "
}

func coreNATSRequester(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) {
	errBytes := []byte("error")
	minusByte := byte('-')

	state := "Requesting"

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		m, err := nc.Request(getPublishSubject(&c, i+offset), msg, time.Second)
		if err != nil {
			log.Fatalf("Request error %v", err)
		}

		if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
			log.Fatalf("Request did not receive a good reply: %q", m.Data)
		}

		time.Sleep(c.sleep)
	}

	state = "Finished  "
}

func jsPublisher(c *benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, idPrefix string, pubNumber string, offset int) {
	js, err := nc.JetStream(jsOpts()...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	var state string

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	if c.batchSize != 1 {
		for i := 0; i < numMsg; {
			state = "Publishing"
			futures := make([]nats.PubAckFuture, min(c.batchSize, numMsg-i))
			for j := 0; j < c.batchSize && (i+j) < numMsg; j++ {
				if c.deDuplication {
					header := nats.Header{}
					header.Set(nats.MsgIdHdr, idPrefix+"-"+pubNumber+"-"+strconv.Itoa(i+j+offset))
					message := nats.Msg{Data: msg, Header: header, Subject: getPublishSubject(c, i+j+offset)}
					futures[j], err = js.PublishMsgAsync(&message)
				} else {
					futures[j], err = js.PublishAsync(getPublishSubject(c, i+j+offset), msg)
				}
				if err != nil {
					log.Fatalf("PubAsync error: %v", err)
				}
				if progress != nil {
					progress.Incr()
				}
				time.Sleep(c.sleep)
			}

			state = "AckWait   "

			select {
			case <-js.PublishAsyncComplete():
				state = "ProcessAck"
				for future := range futures {
					select {
					case <-futures[future].Ok():
						i++
					case err := <-futures[future].Err():
						if err.Error() == "nats: maximum bytes exceeded" {
							log.Fatalf("Stream maximum bytes exceeded, can not publish any more messages")
						}
						log.Printf("PubAsyncFuture for message %v in batch not OK: %v (retrying)", future, err)
						c.retriesUsed = true
					}
				}
			case <-time.After(c.jsTimeout):
				c.retriesUsed = true
				log.Printf("JS PubAsync ack timeout (pending=%d)", js.PublishAsyncPending())
				js, err = nc.JetStream(jsOpts()...)
				if err != nil {
					log.Fatalf("Couldn't get the JetStream context: %v", err)
				}
			}
		}
		state = "Finished  "
	} else {
		state = "Publishing"
		for i := 0; i < numMsg; i++ {
			if progress != nil {
				progress.Incr()
			}
			if c.deDuplication {
				header := nats.Header{}
				header.Set(nats.MsgIdHdr, idPrefix+"-"+pubNumber+"-"+strconv.Itoa(i+offset))
				message := nats.Msg{Data: msg, Header: header, Subject: getPublishSubject(c, i+offset)}
				_, err = js.PublishMsg(&message)
			} else {
				_, err = js.Publish(getPublishSubject(c, i+offset), msg)
			}
			if err != nil {
				if err.Error() == "nats: maximum bytes exceeded" {
					log.Fatalf("Stream maximum bytes exceeded, can not publish any more messages")
				}
				log.Printf("Publish error: %v (retrying)", err)
				c.retriesUsed = true
				i--
			}
			time.Sleep(c.sleep)
		}
	}
}

func kvPutter(c benchCmd, nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) {
	//js, err := nc.JetStream(jsOpts()...)
	//if err != nil {
	//	log.Fatalf("Couldn't get the JetStream context: %v", err)
	//}

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	js, err := getJS(nc)
	if err != nil {
		return
	}

	//if c.newJSAPI {
	kvBucket, err := js.KeyValue(ctx, c.bucketName)
	if err != nil {
		log.Fatalf("Couldn't find kv bucket %s: %v", c.bucketName, err)
	}
	var state = "Putting   "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		_, err = kvBucket.Put(ctx, fmt.Sprintf("%d", offset+i), msg)
		if err != nil {
			log.Fatalf("Put: %s", err)
		}

		time.Sleep(c.sleep)
	}
}

func (c *benchCmd) runCorePublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting publisher, publishing %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.sleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	coreNATSPublisher(*c, nc, progress, msg, numMsg, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runCoreSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	received := 0
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

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

	// Core NATS Message handler
	mh := func(msg *nats.Msg) {
		received++

		if received == 1 {
			ch <- time.Now()
		}

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	state = "Receiving "

	sub, err := nc.Subscribe(getSubscribeSubject(c), mh)
	if err != nil {
		log.Fatalf("Subscribe error: %v", err)
	}

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		log.Fatalf("Error setting pending limits on the subscriber: %v", err)
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}

func (c *benchCmd) runCoreRequester(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting requester, requesting %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.sleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	coreNATSRequester(*c, nc, progress, msg, numMsg, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runServiceServer(nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup) {
	ch := make(chan struct{}, 1)

	log.Print("Starting replier, hit control-c to stop")

	reqHandler := func(request services.Request) {
		time.Sleep(c.sleep)

		err := request.Respond([]byte("ok"))
		if err != nil {
			log.Fatalf("Error replying to the request: %v", err)
		}
	}

	_, err := services.AddService(nc, services.Config{
		Name:    DefaultServiceName,
		Version: DefaultServiceVersion,
		Endpoint: &services.EndpointConfig{
			Subject: c.subject,
			Handler: services.HandlerFunc(reqHandler),
		},
	})
	if err != nil {
		log.Fatalf("AddService error: %v", err)
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	<-ch

	donewg.Done()
}

func (c *benchCmd) runJSPublisher(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, idPrefix string, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting JS publisher, publishing %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.sleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	jsPublisher(c, nc, progress, msg, numMsg, idPrefix, pubNumber, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runJSSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, benchType string, numMsg int) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting subscriber, expecting %s messages", f(numMsg))

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
	mh2 := func(msg jetstream.Msg) {
		received++
		time.Sleep(c.sleep)

		if benchType != BenchTypeJSOrdered {
			if c.ackMode == AckModeExplicit || c.ackMode == AckModeAll {
				err := msg.Ack()
				if err != nil {
					log.Fatalf("Error acknowledging the message: %v", err)
				}
			}
		}

		if received >= numMsg {
			ch <- time.Now()
		}

		if progress != nil {
			progress.Incr()
		}
	}

	var consumer jetstream.Consumer
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	js, err := getJS(nc)
	if err != nil {
		log.Fatalf("Couldn't get JetStream instance: %v", err)
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime
	if progress != nil {
		progress.TimeStarted = startTime
	}

	s, err := js.Stream(ctx, c.streamName)
	if err != nil {
		log.Fatalf("Error getting stream %s: %v", c.streamName, err)
	}

	switch benchType {
	case BenchTypeJSOrdered:
		state = "Receiving"
		consumer, err = s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{InactiveThreshold: time.Second * 10})
		if err != nil {
			log.Fatalf("Error creating the ephemeral ordered consumer: %v", err)
		}

		cc, err := consumer.Consume(mh2)
		if err != nil {
			return
		}
		defer cc.Stop()
	case BenchTypeJSConsume:
		state = "Consuming"
		consumer, err = s.Consumer(ctx, c.consumerName)
		if err != nil {
			log.Fatalf("Error getting consumer %s: %v", c.consumerName, err)
		}

		cc, err := consumer.Consume(mh2, jetstream.PullMaxMessages(c.batchSize), jetstream.StopAfter(numMsg))
		if err != nil {
			return
		}
		defer cc.Stop()
	case BenchTypeJSFetch:
		state = "Fetching"
		consumer, err = s.Consumer(ctx, c.consumerName)
		if err != nil {
			log.Fatalf("Error getting consumer %s: %v", c.consumerName, err)
		}
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	// Fetch messages if in fetch mode
	if benchType == BenchTypeJSFetch {
		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.batchSize <= (numMsg - i) {
					return c.batchSize
				} else {
					return numMsg - i
				}
			}()

			msgs, err := consumer.Fetch(batchSize)
			if err != nil {
				if c.noProgress {
					if err == nats.ErrTimeout {
						log.Print("Fetch  timeout!")
					} else {
						log.Printf("New consumer Fetch error: %v", err)
					}
					c.fetchTimeout = true
				}
			}

			for msg := range msgs.Messages() {
				mh2(msg)
				i++
			}

			if msgs.Error() != nil {
				log.Printf("New consumer Fetch msgs error: %v", msgs.Error())
				c.fetchTimeout = true
			}

		}
	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()

}

func (c *benchCmd) runKVPutter(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, pubNumber string, offset int) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting JS publisher, publishing %s messages", f(numMsg))

	if !c.noProgress {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = progressWidth()
	}

	var msg []byte
	if c.msgSize > 0 {
		msg = make([]byte, c.msgSize)
	}

	<-trigger

	// introduces some jitter between the publishers if pubSleep is set and more than one publisher
	if c.sleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.sleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	kvPutter(*c, nc, progress, msg, numMsg, offset)

	err := nc.Flush()
	if err != nil {
		log.Fatalf("Could not flush the connection: %v", err)
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
}

func (c *benchCmd) runKVGetter(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int) {
	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting KV getter, trying to get %s messages", f(numMsg))

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

	// create the subscriber

	ctx, cancel := context.WithTimeout(context.Background(), c.jsTimeout)
	defer cancel()

	js, err := getJS(nc)
	if err != nil {
		log.Fatalf("Couldn't get JetStream instance: %v", err)
	}

	startwg.Done()

	kvBucket, err := js.KeyValue(ctx, c.bucketName)
	if err != nil {
		log.Fatalf("Couldn't find kv bucket %s: %v", c.bucketName, err)
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime

	if progress != nil {
		progress.TimeStarted = startTime
	}

	state = "Getting   "

	for i := 0; i < numMsg; i++ {
		entry, err := kvBucket.Get(ctx, fmt.Sprintf("%d", offset+i))

		if err != nil {
			log.Fatalf("Error getting key %d: %v", offset+i, err)
		}

		if entry.Value() == nil {
			log.Printf("Warning: got no value for key %d", offset+i)
		}

		if progress != nil {
			progress.Incr()
		}

		time.Sleep(c.sleep)
	}

	ch <- time.Now()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()

}

func (c *benchCmd) runOldJSSubscriber(bm *bench.Benchmark, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, benchType string) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting subscriber, expecting %s messages", f(numMsg))

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
	var mh func(msg *nats.Msg)

	if benchType == BenchTypeOldJSPush || benchType == BenchTypeOldJSPull {
		mh = func(msg *nats.Msg) {
			received++

			time.Sleep(c.sleep)
			if c.ack {
				err := msg.Ack()
				if err != nil {
					log.Fatalf("Error acknowledging the  message: %v", err)
				}
			}

			if received >= numMsg {
				ch <- time.Now()
			}

			if progress != nil {
				progress.Incr()
			}
		}

	} else {
		mh = func(msg *nats.Msg) {
			received++
			time.Sleep(c.sleep)

			if received >= numMsg {
				ch <- time.Now()
			}

			if progress != nil {
				progress.Incr()
			}
		}
	}

	var sub *nats.Subscription

	var err error

	// create the subscriber

	var js nats.JetStreamContext

	js, err = nc.JetStream(jsOpts()...)
	if err != nil {
		log.Fatalf("Couldn't get the JetStream context: %v", err)
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime
	if progress != nil {
		progress.TimeStarted = startTime
	}

	if benchType == BenchTypeOldJSPull {
		sub, err = js.PullSubscribe(getSubscribeSubject(c), c.consumerName, nats.BindStream(c.streamName))
		if err != nil {
			log.Fatalf("Error PullSubscribe: %v", err)
		}
		defer sub.Drain()
	} else if benchType == BenchTypeOldJSPush {
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

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		log.Fatalf("Error setting pending limits on the subscriber: %v", err)
	}

	err = nc.Flush()
	if err != nil {
		log.Fatalf("Error flushing: %v", err)
	}

	startwg.Done()

	if benchType == BenchTypeOldJSPull {
		for i := 0; i < numMsg; {
			batchSize := func() int {
				if c.batchSize <= (numMsg - i) {
					return c.batchSize
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
					i++
				}
			} else {
				if c.noProgress {
					if err == nats.ErrTimeout {
						log.Print("Fetch timeout!")
					} else {
						log.Printf("Pull consumer Fetch error: %v", err)
					}
				}
				c.fetchTimeout = true
			}

		}
	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
}
