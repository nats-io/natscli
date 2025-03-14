// Copyright 2020-2025 The NATS Authors
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
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	iu "github.com/nats-io/natscli/internal/util"

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
	progressBar          bool
	storage              string
	streamOrBucketName   string
	createStream         bool
	streamMaxBytesString string
	streamMaxBytes       int64
	ackMode              string
	doubleAck            bool
	batchSize            int
	replicas             int
	purge                bool
	sleep                time.Duration
	consumerName         string
	history              uint8
	fetchTimeout         bool
	multiSubject         bool
	multiSubjectMax      int
	multisubjectFormat   string
	deDuplication        bool
	deDuplicationWindow  time.Duration
	ack                  bool
	randomizeGets        int
	payloadFilename      string
	hdrs                 []string
}

const (
	benchDefaultDurableConsumerName = "nats-bench"
	benchDefaultStreamName          = "benchstream"
	benchDefaultBucketName          = "benchbucket"
	benchDefaultServiceName         = "nats-bench-service"
	benchDefaultServiceVersion      = "1.0.0"
	benchTypeCorePub                = "pub"
	benchTypeCoreSub                = "sub"
	benchTypeServiceRequest         = "request"
	benchTypeServiceServe           = "reply"
	benchTypeJSPub                  = "jspub"
	benchTypeJSOrdered              = "jsordered"
	benchTypeJSConsume              = "jsconsume"
	benchTypeJSFetch                = "jsfetch"
	benchTypeOldJSOrdered           = "oldjsordered"
	benchTypeOldJSPush              = "oldjspush"
	benchTypeOldJSPull              = "oldjspull"
	benchTypeKVPut                  = "kvput"
	BenchTypeKVGet                  = "kvget"
	benchAckModeNone                = "none"
	benchAckModeAll                 = "all"
	benchAckModeExplicit            = "explicit"
)

func configureBenchCommand(app commandHost) {
	c := &benchCmd{}

	addCommonFlags := func(f *fisk.CmdClause) {
		f.Flag("clients", "Number of concurrent clients").Default("1").IntVar(&c.numClients)
		f.Flag("msgs", "Number of messages to publish or subscribe to").Default("100000").IntVar(&c.numMsg)
		f.Flag("progress", "Enable or disable the progress bar").Default("true").BoolVar(&c.progressBar)
		f.Flag("csv", "Save benchmark data to CSV file").StringVar(&c.csvFile)
		f.Flag("size", "Size of the test messages").Default("128").StringVar(&c.msgSizeString)
		// TODO: support randomized payload data
	}

	addPubFlags := func(f *fisk.CmdClause) {
		f.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)
		f.Flag("multisubjectmax", "The maximum number of subjects to use in multi-subject mode (0 means no max)").Default("100000").IntVar(&c.multiSubjectMax)
		f.Flag("payload", "File containing a message payload to send").ExistingFileVar(&c.payloadFilename)
		f.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	}

	addJSCommonFlags := func(f *fisk.CmdClause) {
		f.Flag("stream", "The name of the stream to create or use").Default(benchDefaultStreamName).StringVar(&c.streamOrBucketName)
		f.Flag("sleep", "Sleep for the specified interval between publications").Default("0s").PlaceHolder("DURATION").DurationVar(&c.sleep)
		f.Flag("purge", "Purge the stream before running").UnNegatableBoolVar(&c.purge)
	}

	addJSConsumerFlags := func(f *fisk.CmdClause) {
		f.Flag("consumer", "Specify the durable consumer name to use").Default(benchDefaultDurableConsumerName).StringVar(&c.consumerName)
		f.Flag("batch", "Sets the max number of messages that can be buffered in the client").Default("500").IntVar(&c.batchSize)
		f.Flag("acks", "Acknowledgement mode for the consumer").Default(benchAckModeExplicit).EnumVar(&c.ackMode, benchAckModeExplicit, benchAckModeNone, benchAckModeAll)
		f.Flag("doubleack", "Synchronously acknowledge messages, waiting for a reply from the server").Default("false").BoolVar(&c.doubleAck)
	}

	addJSPubFlags := func(f *fisk.CmdClause) {
		f.Flag("create", "Create or update the stream first").UnNegatableBoolVar(&c.createStream)
		f.Flag("storage", "JetStream storage (memory/file) for the \"benchstream\" stream").Default("file").EnumVar(&c.storage, "memory", "file")
		f.Flag("replicas", "Number of replicas for the \"benchstream\" stream").Default("1").IntVar(&c.replicas)
		f.Flag("maxbytes", "The maximum size of the stream or KV bucket in bytes").Default("1GB").StringVar(&c.streamMaxBytesString)
		f.Flag("dedup", "Sets a message id in the header to use JS Publish de-duplication").Default("false").UnNegatableBoolVar(&c.deDuplication)
		f.Flag("dedupwindow", "Sets the duration of the stream's deduplication functionality").Default("2m").DurationVar(&c.deDuplicationWindow)
		f.Flag("batch", "The number of asynchronous JS publish calls before waiting for all the publish acknowledgements (set to 1 for synchronous)").Default("500").IntVar(&c.batchSize)
	}

	addKVPutFlags := func(f *fisk.CmdClause) {
		f.Flag("storage", "JetStream storage (memory/file) for the \"benchstream\" bucket").Default("file").EnumVar(&c.storage, "memory", "file")
		f.Flag("replicas", "Number of replicas for the \"benchstream\" bucket").Default("1").IntVar(&c.replicas)
		f.Flag("maxbytes", "The maximum size of the stream or KV bucket in bytes").Default("1GB").StringVar(&c.streamMaxBytesString)
		f.Flag("history", "History depth for the bucket in KV mode").Default("1").Uint8Var(&c.history)
		f.Flag("purge", "Purge the stream before running").UnNegatableBoolVar(&c.purge)
	}

	benchCommand := app.Command("bench", "Benchmark utility")
	addCheat("bench", benchCommand)

	//benchCommand.HelpLong(benchHelp)

	corePub := benchCommand.Command("pub", "Publish Core NATS messages").Action(c.pubAction)
	corePub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	corePub.Flag("sleep", "Sleep for the specified interval between publications").Default("0s").PlaceHolder("DURATION").DurationVar(&c.sleep)
	addCommonFlags(corePub)
	addPubFlags(corePub)

	coreSub := benchCommand.Command("sub", "Subscribe to Core NATS messages").Action(c.subAction)
	coreSub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	coreSub.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)
	addCommonFlags(coreSub)

	microService := benchCommand.Command("service", "Micro-service mode")
	microService.Flag("sleep", "Sleep for the specified interval between requests or before replying to the request").Default("0s").PlaceHolder("DURATION").DurationVar(&c.sleep)
	addCommonFlags(microService)

	request := microService.Command("request", "Send a request and wait for its reply").Action(c.requestAction)
	request.Help("Send a request and wait for a reply")
	request.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	request.Flag("payload", "File containing the payload to send").ExistingFileVar(&c.payloadFilename)
	request.Flag("header", "Adds headers to the message using K:V format").Short('H').StringsVar(&c.hdrs)
	// TODO: support randomized payload data

	reply := microService.Command("serve", "Service requests").Action(c.serveAction)
	reply.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)

	jsCommand := benchCommand.Command("js", "JetStream benchmark commands")
	addCommonFlags(jsCommand)
	addJSCommonFlags(jsCommand)

	jspub := jsCommand.Command("pub", "Publish JetStream messages").Action(c.jspubAction)
	jspub.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	addPubFlags(jspub)
	addJSPubFlags(jspub)

	jsOrdered := jsCommand.Command("ordered", "Consume JetStream messages from a consumer using an ephemeral ordered consumer").Action(c.jsOrderedAction)
	jsOrdered.Flag("batch", "Sets the max number of messages that can be buffered in the client").Default("500").IntVar(&c.batchSize)

	jsConsume := jsCommand.Command("consume", "Consume JetStream messages from a durable consumer using a callback").Action(c.jsConsumeAction)
	addJSConsumerFlags(jsConsume)

	jsFetch := jsCommand.Command("fetch", "Consume JetStream messages from a durable consumer using fetch").Action(c.jsFetchAction)
	addJSConsumerFlags(jsFetch)

	kvCommand := benchCommand.Command("kv", "KV benchmark operations")
	addCommonFlags(kvCommand)
	kvCommand.Flag("bucket", "The bucket to use for the benchmark").Default(benchDefaultBucketName).StringVar(&c.streamOrBucketName)
	kvCommand.Flag("sleep", "Sleep for the specified interval after putting each message").Default("0s").PlaceHolder("DURATION").DurationVar(&c.sleep)

	kvput := kvCommand.Command("put", "Put messages in a KV bucket").Action(c.kvPutAction)
	// TODO: support randomized payload data
	addKVPutFlags(kvput)

	kvget := kvCommand.Command("get", "Get messages from a KV bucket").Action(c.kvGetAction)
	kvget.Flag("randomize", "Randomly access messages using keys between 0 and this number (set to 0 for sequential access)").Default("0").IntVar(&c.randomizeGets)

	oldJSCommand := benchCommand.Command("oldjs", "JetStream benchmark commands using the old JS API").Hidden()
	addCommonFlags(oldJSCommand)
	addJSCommonFlags(oldJSCommand)

	oldJSOrdered := oldJSCommand.Command("ordered", "Consume JetStream messages from a consumer using an old JS API's ephemeral ordered consumer").Action(c.oldjsOrderedAction)
	oldJSOrdered.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	oldJSOrdered.Flag("multisubject", "Multi-subject mode, each message is published on a subject that includes the publisher's message sequence number as a token").UnNegatableBoolVar(&c.multiSubject)

	oldJSPush := oldJSCommand.Command("push", "Consume JetStream messages from a consumer using an old JS API's durable push consumer").Action(c.oldjsPushAction)
	oldJSPush.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	oldJSPush.Flag("consumer", "Specify the durable consumer name to use").Default(benchDefaultDurableConsumerName).StringVar(&c.consumerName)
	oldJSPush.Flag("maxacks", "Sets the max ack pending value, adjusts for the number of clients").Default("500").IntVar(&c.batchSize)
	oldJSPush.Flag("ack", "Uses explicit message acknowledgement or not for the consumer").Default("true").BoolVar(&c.ack)
	oldJSPush.Flag("doubleack", "Synchronously acknowledge messages, waiting for a reply from the server").Default("false").BoolVar(&c.doubleAck)

	oldJSPull := oldJSCommand.Command("pull", "Consume JetStream messages from a consumer using an old JS API's durable pull consumer").Action(c.oldjsPullAction)
	oldJSPull.Arg("subject", "Subject to use for the benchmark").Required().StringVar(&c.subject)
	oldJSPull.Flag("consumer", "Specify the durable consumer name to use").Default(benchDefaultDurableConsumerName).StringVar(&c.consumerName)
	oldJSPull.Flag("batch", "Sets the fetch size for the consumer").Default("500").IntVar(&c.batchSize)
	oldJSPull.Flag("ack", "Uses explicit message acknowledgement or not for the consumer").Default("true").BoolVar(&c.ack)
	oldJSPull.Flag("doubleack", "Synchronously acknowledge messages, waiting for a reply from the server").Default("false").BoolVar(&c.doubleAck)

}

func init() {
	registerCommand("bench", 2, configureBenchCommand)
}

func (c *benchCmd) getJS(nc *nats.Conn) (jetstream.JetStream, error) {
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
		return nil, fmt.Errorf("getting the new API JetStream instance: %w", err)
	}

	return js, nil
}

func (c *benchCmd) offset(putter int, counts []int) int {
	var position = 0

	for i := 0; i < putter; i++ {
		position = position + counts[i]
	}
	return position
}

func (c *benchCmd) processActionArgs() error {
	if c.numMsg <= 0 {
		return fmt.Errorf("number of messages should be greater than 0")
	}

	// for pubs/request/and put only
	if c.msgSizeString != "" {
		msgSize, err := iu.ParseStringAsBytes(c.msgSizeString)
		if err != nil || msgSize <= 0 || msgSize > math.MaxInt {
			return fmt.Errorf("can not parse or invalid the value specified for the message size: %s", c.msgSizeString)
		} else {
			c.msgSize = int(msgSize)
		}
	}

	if opts().Config == nil {
		return fmt.Errorf("unknown context %q", opts().CfgCtx)
	}

	if c.streamMaxBytesString != "" {
		size, err := iu.ParseStringAsBytes(c.streamMaxBytesString)
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
		if c.createStream {
			argnvps = append(argnvps, nvp{"storage", c.storage})
			argnvps = append(argnvps, nvp{"max-bytes", f(uint64(c.streamMaxBytes))})
			argnvps = append(argnvps, nvp{"replicas", f(c.replicas)})
			argnvps = append(argnvps, nvp{"deduplication", f(c.deDuplication)})
			argnvps = append(argnvps, nvp{"dedup-window", f(c.deDuplicationWindow)})
		}
	}

	jsAttributes := func() {
		argnvps = append(argnvps, nvp{"stream", f(c.streamOrBucketName)})
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	}

	benchTypeLabel := "Unknown benchmark"

	switch benchType {
	case benchTypeCorePub:
		benchTypeLabel = "Core NATS publish"
		argnvps = append(argnvps, nvp{"subject", c.getSubscribeSubject()})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case benchTypeCoreSub:
		benchTypeLabel = "Core NATS subscribe"
		argnvps = append(argnvps, nvp{"subject", c.getSubscribeSubject()})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
	case benchTypeServiceRequest:
		benchTypeLabel = "Core NATS service request"
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case benchTypeServiceServe:
		benchTypeLabel = "Core NATS service serve"
		argnvps = append(argnvps, nvp{"subject", c.subject})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
	case benchTypeJSPub:
		benchTypeLabel = "JetStream publish"
		argnvps = append(argnvps, nvp{"subject", c.getSubscribeSubject()})
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		argnvps = append(argnvps, nvp{"multi-subject-max", f(c.multiSubjectMax)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		jsAttributes()
		streamOrBucketAttribues()
	case benchTypeJSOrdered:
		benchTypeLabel = "JetStream ordered ephemeral consumer"
		jsAttributes()
		streamOrBucketAttribues()
	case benchTypeJSConsume:
		benchTypeLabel = "JetStream durable consumer (callback)"
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"acks", c.ackMode})
		argnvps = append(argnvps, nvp{"double-acked", f(c.doubleAck)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		jsAttributes()
		streamOrBucketAttribues()
	case benchTypeJSFetch:
		benchTypeLabel = "JetStream durable consumer (fetch)"
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"acks", c.ackMode})
		argnvps = append(argnvps, nvp{"double-acked", f(c.doubleAck)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		jsAttributes()
		streamOrBucketAttribues()
	case benchTypeKVPut:
		benchTypeLabel = "KV put"
		argnvps = append(argnvps, nvp{"bucket", c.streamOrBucketName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"purge", f(c.purge)})
		streamOrBucketAttribues()
	case BenchTypeKVGet:
		benchTypeLabel = "KV get"
		argnvps = append(argnvps, nvp{"bucket", c.streamOrBucketName})
		argnvps = append(argnvps, nvp{"sleep", f(c.sleep)})
		argnvps = append(argnvps, nvp{"randomize", f(c.randomizeGets)})
		streamOrBucketAttribues()
	case benchTypeOldJSOrdered:
		benchTypeLabel = "old JetStream API ordered ephemeral consumer"
		argnvps = append(argnvps, nvp{"multi-subject", f(c.multiSubject)})
		jsAttributes()
		streamOrBucketAttribues()
	case benchTypeOldJSPush:
		benchTypeLabel = "old JetStream API durable push consumer"
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"acked", fmt.Sprintf("%v", c.ack)})
		argnvps = append(argnvps, nvp{"double-acked", f(c.doubleAck)})
		jsAttributes()
		streamOrBucketAttribues()
	case benchTypeOldJSPull:
		benchTypeLabel = "old JetStream API durable pull consumer"
		argnvps = append(argnvps, nvp{"consumer", c.consumerName})
		argnvps = append(argnvps, nvp{"acked", fmt.Sprintf("%v", c.ack)})
		argnvps = append(argnvps, nvp{"double-acked", f(c.doubleAck)})
		argnvps = append(argnvps, nvp{"batch", f(c.batchSize)})
		jsAttributes()
		streamOrBucketAttribues()
	}

	argnvps = append(argnvps, nvp{"msgs", f(c.numMsg)})
	argnvps = append(argnvps, nvp{"msg-size", humanize.IBytes(uint64(c.msgSize))})
	argnvps = append(argnvps, nvp{"clients", f(c.numClients)})

	banner := fmt.Sprintf("Starting %s benchmark [", benchTypeLabel)

	var joinBuffer []string

	sort.Slice(argnvps, func(i, j int) bool {
		return argnvps[i].name < argnvps[j].name
	})

	for _, v := range argnvps {
		joinBuffer = append(joinBuffer, v.name+"="+v.value)
	}

	banner += strings.Join(joinBuffer, ", ") + "]"

	return banner
}

func (c *benchCmd) printResults(bm *bench.Benchmark) error {
	if c.progressBar {
		uiprogress.Stop()
	}

	if c.fetchTimeout {
		log.Print("WARNING: at least one of the pull consumer Fetch operation timed out. These results are not optimal!")
	}

	fmt.Println()
	fmt.Println(bm.Report())

	if c.csvFile != "" {
		csv := bm.CSV()
		err := os.WriteFile(c.csvFile, []byte(csv), 0600)
		if err != nil {
			return fmt.Errorf("writing file %s: %w", c.csvFile, err)
		}
		fmt.Printf("Saved metric data in csv file %s\n", c.csvFile)
	}

	return nil
}

func (c *benchCmd) getSubscribeSubject() string {
	if c.multiSubject {
		return c.subject + ".*"
	} else {
		return c.subject
	}
}

func (c *benchCmd) getPublishSubject(number int) string {
	if c.multiSubject {
		if c.multiSubjectMax == 0 {
			return c.subject + "." + strconv.Itoa(number)
		} else {
			return c.subject + "." + fmt.Sprintf(c.multisubjectFormat, number%c.multiSubjectMax)
		}
	} else {
		return c.subject
	}
}

func (c *benchCmd) storageType() jetstream.StorageType {
	if c.storage == "memory" {
		return jetstream.MemoryStorage
	} else {
		return jetstream.FileStorage
	}
}

func (c *benchCmd) createOrUpdateConsumer(js jetstream.JetStream) error {
	var ack jetstream.AckPolicy

	switch c.ackMode {
	case benchAckModeNone:
		ack = jetstream.AckNonePolicy
	case benchAckModeAll:
		ack = jetstream.AckAllPolicy
	case benchAckModeExplicit:
		ack = jetstream.AckExplicitPolicy
	}

	_, err := js.CreateOrUpdateConsumer(ctx, c.streamOrBucketName, jetstream.ConsumerConfig{
		Durable:           c.consumerName,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         ack,
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		MaxAckPending:     c.batchSize * c.numClients,
		InactiveThreshold: time.Second * 10,
	})
	if err != nil {
		return fmt.Errorf("creating the durable consumer '%s': %w", c.consumerName, err)
	}

	return nil
}

func (c *benchCmd) purgeStream() error {
	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	s, err := js.Stream(ctx, c.streamOrBucketName)
	if err != nil {
		return fmt.Errorf("getting stream '%s': %w", c.streamOrBucketName, err)
	}

	err = s.Purge(ctx)
	if err != nil {
		return fmt.Errorf("purging stream '%s': %w", c.streamOrBucketName, err)
	}

	return nil
}

// Actions for the various bench commands below
func (c *benchCmd) pubAction(_ *fisk.ParseContext) error {
	err := c.processActionArgs()
	if err != nil {
		return err
	}

	banner := c.generateBanner(benchTypeCorePub)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return err
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCorePublisher(bm, errChan, nc, startwg, donewg, trigger, pubCounts[i], c.offset(i, pubCounts), strconv.Itoa(i))
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeCoreSub)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d failed to connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreSubscriber(bm, errChan, nc, startwg, donewg, c.numMsg)
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeServiceRequest)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d failed to connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runCoreRequester(bm, errChan, nc, startwg, donewg, trigger, pubCounts[i], c.offset(i, pubCounts), strconv.Itoa(i))
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeServiceServe)

	log.Println(banner)

	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d failed to connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runServiceServer(nc, errChan, startwg, donewg)
	}

	startwg.Wait()
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeJSPub)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", 0, c.numClients)
	benchId := strconv.FormatInt(time.Now().UnixMilli(), 16)
	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	// create the stream for the benchmark (and purge it)
	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return err
	}

	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	var s jetstream.Stream

	if c.createStream {
		// create the stream with our attributes, will create it if it doesn't exist or make sure the existing one has the same attributes
		s, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: c.streamOrBucketName, Subjects: []string{c.getSubscribeSubject()}, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, Storage: c.storageType(), Replicas: c.replicas, MaxBytes: c.streamMaxBytes, Duplicates: c.deDuplicationWindow})
		if err != nil {
			return fmt.Errorf("could not create the stream. If you want to delete and re-define the stream use `nats stream delete %s`: %w", c.streamOrBucketName, err)
		}
		// TODO: a way to wait for the stream to be ready (e.g. when updating the stream's config (e.g. from R1 to R3))
	} else {
		s, err = js.Stream(ctx, c.streamOrBucketName)
		if err != nil {
			return fmt.Errorf("stream '%s' does not exist, create it with --create", c.streamOrBucketName)
		}
		log.Printf("Using stream: %s", c.streamOrBucketName)
	}

	if c.purge {
		log.Printf("Purging the stream")
		err = s.Purge(ctx)
		if err != nil {
			return err
		}
	}

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return err
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSPublisher(bm, errChan, nc, startwg, donewg, trigger, pubCounts[i], c.offset(i, pubCounts), benchId, strconv.Itoa(i))
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Fatal error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeJSOrdered)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, errChan, nc, startwg, donewg, benchTypeJSOrdered, c.numMsg)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeJSConsume)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	if c.consumerName == benchDefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it just delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, errChan, nc, startwg, donewg, benchTypeJSConsume, subCounts[i])
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeJSFetch)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	if c.consumerName == benchDefaultDurableConsumerName {
		// create the consumer
		// TODO: Should it be just create or delete and create each time?
		err = c.createOrUpdateConsumer(js)
		if err != nil {
			return err
		}
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runJSSubscriber(bm, errChan, nc, startwg, donewg, benchTypeJSFetch, subCounts[i])
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeKVPut)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", 0, c.numClients)

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer nc.Close()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	// There is no way to purge all the keys in a KV bucket in a single operation so deleting the bucket instead
	if c.purge {
		err = js.DeleteKeyValue(ctx, c.streamOrBucketName)
		if err != nil {
			return err
		}
	}

	if c.streamOrBucketName == benchDefaultBucketName {
		// create bucket
		_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: c.streamOrBucketName, History: c.history, Storage: c.storageType(), Description: "nats bench bucket", Replicas: c.replicas, MaxBytes: c.streamMaxBytes})
		if err != nil {
			return err
		}
	}

	startwg.Wait()

	pubCounts := bench.MsgsPerClient(c.numMsg, c.numClients)
	trigger := make(chan struct{})
	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return err
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVPutter(bm, errChan, nc, startwg, donewg, trigger, pubCounts[i], strconv.Itoa(i), c.offset(i, pubCounts))
	}

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	close(trigger)
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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
	errChan := make(chan error, c.numClients)

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d cloud not connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runKVGetter(bm, errChan, nc, startwg, donewg, subCounts[i], c.offset(i, subCounts))
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	startwg.Wait()
	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeOldJSOrdered)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, errChan, nc, startwg, donewg, c.numMsg, benchTypeOldJSOrdered)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeOldJSPush)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return err
	}

	js, err := nc.JetStream(append(jsOpts(), nats.MaxWait(opts().Timeout))...)
	if err != nil {
		return err
	}

	if c.consumerName == benchDefaultDurableConsumerName {
		ack := nats.AckNonePolicy
		if c.ack {
			ack = nats.AckExplicitPolicy
		}
		maxAckPending := 0
		if c.ack {
			maxAckPending = c.batchSize * c.numClients
		}
		_, err = js.AddConsumer(c.streamOrBucketName, &nats.ConsumerConfig{
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
			err := js.DeleteConsumer(c.streamOrBucketName, c.consumerName)
			if err != nil {
				log.Printf("Error deleting the durable push consumer on stream %s: %v", c.streamOrBucketName, err)
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
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, errChan, nc, startwg, donewg, subCounts[i], benchTypeOldJSPush)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

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

	banner := c.generateBanner(benchTypeOldJSPull)
	log.Println(banner)
	bm := bench.NewBenchmark("NATS", c.numClients, 0)

	if c.purge {
		err = c.purgeStream()
		if err != nil {
			return err
		}
	}

	startwg := &sync.WaitGroup{}
	donewg := &sync.WaitGroup{}
	errChan := make(chan error, c.numClients)

	nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}

	js, err := nc.JetStream(append(jsOpts(), nats.MaxWait(opts().Timeout))...)
	if err != nil {
		return fmt.Errorf("getting the JetStream context: %w", err)
	}

	ack := nats.AckNonePolicy
	if c.ack {
		ack = nats.AckExplicitPolicy
	}

	if c.consumerName == benchDefaultDurableConsumerName {
		_, err = js.AddConsumer(c.streamOrBucketName, &nats.ConsumerConfig{
			Durable:       c.consumerName,
			DeliverPolicy: nats.DeliverAllPolicy,
			AckPolicy:     ack,
			ReplayPolicy:  nats.ReplayInstantPolicy,
			MaxAckPending: min(c.numClients*c.batchSize, 10000),
		})
		if err != nil {
			return fmt.Errorf("creating the durable consumer '%s': %w", c.consumerName, err)
		}
		defer func() {
			err := js.DeleteConsumer(c.streamOrBucketName, c.consumerName)
			if err != nil {
				log.Printf("Error deleting the pull consumer on stream %s: %v", c.streamOrBucketName, err)
			}
			log.Printf("Deleted durable consumer: %s\n", c.consumerName)
		}()
		log.Printf("Defined durable pull consumer: %s\n", c.consumerName)
	}

	subCounts := bench.MsgsPerClient(c.numMsg, c.numClients)

	for i := 0; i < c.numClients; i++ {
		nc, err := nats.Connect(opts().Config.ServerURL(), natsOpts()...)
		if err != nil {
			return fmt.Errorf("client number %d could not connect: %w", i, err)
		}
		defer nc.Close()

		startwg.Add(1)
		donewg.Add(1)

		go c.runOldJSSubscriber(bm, errChan, nc, startwg, donewg, subCounts[i], benchTypeOldJSPull)
	}
	startwg.Wait()

	if c.progressBar {
		uiprogress.Start()
	}

	donewg.Wait()

	var err2 error
	for i := 0; i < c.numClients; i++ {
		if err := <-errChan; err != nil {
			log.Printf("Error from client %d: %v", i, err)
			// only return the first error since only one error can be returned
			if err2 == nil {
				err2 = err
			}
		}
	}

	if err2 != nil {
		return err2
	}

	bm.Close()
	err = c.printResults(bm)
	if err != nil {
		return err
	}

	return nil
}

func (c benchCmd) getPayload(msgSize int) ([]byte, error) {
	if len(c.payloadFilename) > 0 {

		buffer, err := os.ReadFile(c.payloadFilename)
		if err != nil {
			return nil, fmt.Errorf("reading the payload file: %w", err)
		}

		return buffer, nil
	}

	buffer := make([]byte, msgSize)
	return buffer, nil
}

func (c *benchCmd) coreNATSPublisher(nc *nats.Conn, progress *uiprogress.Bar, payloadSize int, numMsg int, offset int) error {
	state := "Publishing"

	payload, err := c.getPayload(payloadSize)
	if err != nil {
		return err
	}

	headers, err := iu.ParseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return err
	}

	message := nats.Msg{Data: payload, Header: headers}

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	c.multisubjectFormat = fmt.Sprintf("%%0%dd", len(strconv.Itoa(c.multiSubjectMax)))

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		message.Subject = c.getPublishSubject(i + offset)
		err := nc.PublishMsg(&message)
		if err != nil {
			return fmt.Errorf("publishing: %w", err)
		}

		time.Sleep(c.sleep)
	}

	state = "Finished  "
	return nil
}

func (c *benchCmd) coreNATSRequester(nc *nats.Conn, progress *uiprogress.Bar, payloadSize int, numMsg int, offset int) error {
	errBytes := []byte("error")
	minusByte := byte('-')

	state := "Requesting"
	payload, err := c.getPayload(payloadSize)
	if err != nil {
		return err
	}

	headers, err := iu.ParseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return err
	}

	message := nats.Msg{Data: payload, Header: headers}

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	c.multisubjectFormat = fmt.Sprintf("%%0%dd", len(strconv.Itoa(c.multiSubjectMax)))

	for i := 0; i < numMsg; i++ {
		if progress != nil {
			progress.Incr()
		}

		message.Subject = c.getPublishSubject(i + offset)

		m, err := nc.RequestMsg(&message, opts().Timeout)
		if err != nil {
			return fmt.Errorf("requesting: %w", err)
		}

		if len(m.Data) == 0 || m.Data[0] == minusByte || bytes.Contains(m.Data, errBytes) {
			log.Fatalf("Request did not receive a good reply: %q", m.Data)
		}

		time.Sleep(c.sleep)
	}

	state = "Finished  "
	return nil
}

func (c *benchCmd) jsPublisher(nc *nats.Conn, progress *uiprogress.Bar, payloadSize int, numMsg int, idPrefix string, pubNumber string, offset int) error {
	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	var state string
	payload, err := c.getPayload(payloadSize)
	if err != nil {
		return err
	}

	headers, err := iu.ParseStringsToHeader(c.hdrs, 0)
	if err != nil {
		return err
	}

	message := nats.Msg{Data: payload, Header: headers}

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	c.multisubjectFormat = fmt.Sprintf("%%0%dd", len(strconv.Itoa(c.multiSubjectMax)))

	if c.batchSize != 1 {
		for i := 0; i < numMsg; {
			state = "Publishing"
			futures := make([]jetstream.PubAckFuture, min(c.batchSize, numMsg-i))
			for j := 0; j < c.batchSize && (i+j) < numMsg; j++ {
				if c.deDuplication {
					message.Header.Set(nats.MsgIdHdr, idPrefix+"-"+pubNumber+"-"+strconv.Itoa(i+j+offset))
				}

				message.Subject = c.getPublishSubject(i + j + offset)

				futures[j], err = js.PublishMsgAsync(&message)
				if err != nil {
					return fmt.Errorf("publishing asynchronously: %w", err)
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
						fmt.Println(fmt.Errorf("publish acknowledgement is an error: %w (retrying)", err).Error())
					}
				}
			case <-time.After(opts().Timeout):
				return fmt.Errorf("JS PubAsync ack timeout (pending=%d)", js.PublishAsyncPending())
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
				message.Header.Set(nats.MsgIdHdr, idPrefix+"-"+pubNumber+"-"+strconv.Itoa(i+offset))
			}

			message.Subject = c.getPublishSubject(i + offset)

			_, err = js.PublishMsg(ctx, &message)
			if err != nil {
				return fmt.Errorf("publishing synchronously: %w", err)
			}
			time.Sleep(c.sleep)
		}
	}
	return nil
}

func (c *benchCmd) kvPutter(nc *nats.Conn, progress *uiprogress.Bar, msg []byte, numMsg int, offset int) error {
	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		return err
	}

	//if c.newJSAPI {
	kvBucket, err := js.KeyValue(ctx, c.streamOrBucketName)
	if err != nil {
		return fmt.Errorf("getting the kv bucket '%s': %w", c.streamOrBucketName, err)
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
			return fmt.Errorf("putting: %w", err)
		}

		time.Sleep(c.sleep)
	}
	return nil
}

func (c *benchCmd) runCorePublisher(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting publisher, publishing %s messages", f(numMsg))

	if c.progressBar {
		barTotal := numMsg
		if barTotal == 0 {
			barTotal = 1
		}

		progress = uiprogress.AddBar(barTotal).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()

		if numMsg == 0 {
			progress.PrependFunc(func(b *uiprogress.Bar) string {
				return "Finished  "
			})
			progress.Incr()
		}
	}

	if numMsg == 0 {
		donewg.Done()
		errChan <- nil
		return
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.sleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	err := c.coreNATSPublisher(nc, progress, c.msgSize, numMsg, offset)
	if err != nil {
		errChan <- fmt.Errorf("publishing: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runCoreSubscriber(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int) {
	received := 0
	ch := make(chan time.Time, 2)
	var progress *uiprogress.Bar

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
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

	sub, err := nc.Subscribe(c.getSubscribeSubject(), mh)
	if err != nil {
		errChan <- fmt.Errorf("subscribing to '%s': %w", c.getSubscribeSubject(), err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		errChan <- fmt.Errorf("setting pending limits on the subscriber: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runCoreRequester(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting requester, requesting %s messages", f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.sleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	err := c.coreNATSRequester(nc, progress, c.msgSize, numMsg, offset)
	if err != nil {
		errChan <- fmt.Errorf("requesting: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runServiceServer(nc *nats.Conn, errChan chan error, startwg *sync.WaitGroup, donewg *sync.WaitGroup) {
	ch := make(chan struct{}, 1)

	log.Print("Starting replier, hit control-c to stop")

	reqHandler := func(request services.Request) {
		time.Sleep(c.sleep)

		err := request.Respond([]byte("ok"))
		if err != nil {
			errChan <- fmt.Errorf("replying to the request: %w", err)
			donewg.Done()
			return
		}
	}

	_, err := services.AddService(nc, services.Config{
		Name:    benchDefaultServiceName,
		Version: benchDefaultServiceVersion,
		Endpoint: &services.EndpointConfig{
			Subject: c.subject,
			Handler: services.HandlerFunc(reqHandler),
		},
	})
	if err != nil {
		errChan <- fmt.Errorf("adding the service: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	<-ch

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runJSPublisher(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, offset int, idPrefix string, pubNumber string) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting JS publisher, publishing %s messages", f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	<-trigger

	// introduces some jitter between the publishers if sleep is set and more than one publisher
	if c.sleep != 0 && pubNumber != "0" {
		n := rand.Intn(int(c.sleep))
		time.Sleep(time.Duration(n))
	}

	start := time.Now()
	err := c.jsPublisher(nc, progress, c.msgSize, numMsg, idPrefix, pubNumber, offset)
	if err != nil {
		errChan <- fmt.Errorf("publishing: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runJSSubscriber(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, benchType string, numMsg int) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting subscriber, expecting %s messages", f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Message handler
	mh := func(msg jetstream.Msg) {
		received++
		time.Sleep(c.sleep)

		if benchType != benchTypeJSOrdered {
			if c.ackMode == benchAckModeExplicit || c.ackMode == benchAckModeAll {
				var err error
				if c.doubleAck {
					err = msg.DoubleAck(ctx)
				} else {
					err = msg.Ack()
				}
				if err != nil {
					errChan <- fmt.Errorf("acknowledging the message: %w", err)
					donewg.Done()
					return
				}
			}
		}

		if received == 1 {
			startTime := time.Now()
			ch <- startTime

			if progress != nil {
				progress.TimeStarted = startTime
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

	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		errChan <- fmt.Errorf("getting the JetStream instance: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	s, err := js.Stream(ctx, c.streamOrBucketName)
	if err != nil {
		errChan <- fmt.Errorf("getting stream '%s': %w", c.streamOrBucketName, err)
		startwg.Done()
		donewg.Done()
		return
	}

	switch benchType {
	case benchTypeJSOrdered:
		state = "Receiving"
		consumer, err = s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{InactiveThreshold: time.Second * 10})
		if err != nil {
			errChan <- fmt.Errorf("creating the ephemeral ordered consumer: %w", err)
			startwg.Done()
			donewg.Done()
			return
		}

		cc, err := consumer.Consume(mh, jetstream.PullMaxMessages(c.batchSize))
		if err != nil {
			errChan <- fmt.Errorf("calling Consume() on the ordered consumer: %w", err)
			startwg.Done()
			donewg.Done()
			return
		}
		defer cc.Stop()
	case benchTypeJSConsume:
		state = "Consuming"
		consumer, err = s.Consumer(ctx, c.consumerName)
		if err != nil {
			errChan <- fmt.Errorf("getting durable consumer '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}

		cc, err := consumer.Consume(mh, jetstream.PullMaxMessages(c.batchSize), jetstream.StopAfter(numMsg))
		if err != nil {
			errChan <- fmt.Errorf("calling Consume() on the durable consumer '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}
		defer cc.Stop()
	case benchTypeJSFetch:
		state = "Fetching"
		consumer, err = s.Consumer(ctx, c.consumerName)
		if err != nil {
			errChan <- fmt.Errorf("getting durable consumer '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	// Fetch messages if in fetch mode
	if benchType == benchTypeJSFetch {
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
				if !c.progressBar {
					if err == nats.ErrTimeout {
						log.Print("Fetch  timeout!")
					} else {
						errChan <- fmt.Errorf("fetching from the consumer '%s': %w", c.consumerName, err)
						donewg.Done()
						return
					}
					c.fetchTimeout = true
				}
			}

			for msg := range msgs.Messages() {
				mh(msg)
				i++
			}

			if msgs.Error() != nil {
				errChan <- fmt.Errorf("getting fetched messages: %w", msgs.Error())
				c.fetchTimeout = true
				donewg.Done()
				return
			}

		}
	}

	start := <-ch
	end := <-ch

	state = "Finished  "

	bm.AddSubSample(bench.NewSample(numMsg, c.msgSize, start, end, nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runKVPutter(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, trigger chan struct{}, numMsg int, pubNumber string, offset int) {
	startwg.Done()

	var progress *uiprogress.Bar

	log.Printf("Starting JS publisher, publishing %s messages", f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
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
	err := c.kvPutter(nc, progress, msg, numMsg, offset)
	if err != nil {
		errChan <- fmt.Errorf("putting: %w", err)
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		donewg.Done()
		return
	}

	bm.AddPubSample(bench.NewSample(numMsg, c.msgSize, start, time.Now(), nc))

	donewg.Done()
	errChan <- nil
}

func (c *benchCmd) runKVGetter(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, offset int) {
	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting KV getter, trying to get %s messages", f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// create the subscriber

	ctx := context.Background()

	js, err := c.getJS(nc)
	if err != nil {
		errChan <- fmt.Errorf("getting the JetStream instance: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	kvBucket, err := js.KeyValue(ctx, c.streamOrBucketName)
	if err != nil {
		errChan <- fmt.Errorf("finding kv bucket '%s': %w", c.streamOrBucketName, err)
		donewg.Done()
		return
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime

	if progress != nil {
		progress.TimeStarted = startTime
	}

	state = "Getting   "

	for i := 0; i < numMsg; i++ {
		var key string

		if c.randomizeGets == 0 {
			key = fmt.Sprintf("%d", offset+i)
		} else {
			key = fmt.Sprintf("%d", rand.Intn(c.randomizeGets))
		}
		entry, err := kvBucket.Get(ctx, key)

		if err != nil {
			errChan <- fmt.Errorf("getting key '%s': %w", key, err)
			donewg.Done()
			return
		}

		if entry.Value() == nil {
			log.Printf("Warning: got no value for key '%d'", offset+i)
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
	errChan <- nil
}

func (c *benchCmd) runOldJSSubscriber(bm *bench.Benchmark, errChan chan error, nc *nats.Conn, startwg *sync.WaitGroup, donewg *sync.WaitGroup, numMsg int, benchType string) {
	received := 0

	ch := make(chan time.Time, 2)

	var progress *uiprogress.Bar

	log.Printf("Starting subscriber, expecting %s messages", f(numMsg))

	if c.progressBar {
		progress = uiprogress.AddBar(numMsg).AppendCompleted().PrependElapsed()
		progress.Width = iu.ProgressWidth()
	}

	state := "Setup     "

	if progress != nil {
		progress.PrependFunc(func(b *uiprogress.Bar) string {
			return state
		})
	}

	// Message handler
	var mh func(msg *nats.Msg)

	if benchType == benchTypeOldJSPush || benchType == benchTypeOldJSPull {
		mh = func(msg *nats.Msg) {
			received++

			time.Sleep(c.sleep)
			if c.ack {
				var err error
				if c.doubleAck {
					err = msg.AckSync()
				} else {
					err = msg.Ack()
				}
				if err != nil {
					errChan <- fmt.Errorf("acknowledging the message: %w", err)
					donewg.Done()
					return
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
		errChan <- fmt.Errorf("getting the JetStream context: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	// start the timer now rather than when the first message is received in JS mode
	startTime := time.Now()
	ch <- startTime
	if progress != nil {
		progress.TimeStarted = startTime
	}

	if benchType == benchTypeOldJSPull {
		sub, err = js.PullSubscribe(c.getSubscribeSubject(), c.consumerName, nats.BindStream(c.streamOrBucketName))
		if err != nil {
			errChan <- fmt.Errorf("PullSubscribe: %w", err)
			startwg.Done()
			donewg.Done()
			return
		}
		defer func(sub *nats.Subscription) {
			err := sub.Drain()
			if err != nil {
				log.Printf("draining the subscription at the end of the run: %v", err)
			}
		}(sub)
	} else if benchType == benchTypeOldJSPush {
		state = "Receiving "
		sub, err = js.QueueSubscribe(c.getSubscribeSubject(), c.consumerName+"-GROUP", mh, nats.Bind(c.streamOrBucketName, c.consumerName), nats.ManualAck())
		if err != nil {
			errChan <- fmt.Errorf("subscribing to the push durable '%s': %w", c.consumerName, err)
			startwg.Done()
			donewg.Done()
			return
		}
		_ = sub.AutoUnsubscribe(numMsg)

	} else { // benchType == benchTypeOldJSOrdered
		state = "Consuming "
		// ordered push consumer
		sub, err = js.Subscribe(c.getSubscribeSubject(), mh, nats.OrderedConsumer())
		if err != nil {
			errChan <- fmt.Errorf("subscribing to the ordered consumer on subject '%s': %w", c.getSubscribeSubject(), err)
			startwg.Done()
			donewg.Done()
			return
		}
	}

	err = sub.SetPendingLimits(-1, -1)
	if err != nil {
		errChan <- fmt.Errorf("setting pending limits on the subscriber: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	err = nc.Flush()
	if err != nil {
		errChan <- fmt.Errorf("flushing: %w", err)
		startwg.Done()
		donewg.Done()
		return
	}

	startwg.Done()

	if benchType == benchTypeOldJSPull {
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

			msgs, err := sub.Fetch(batchSize, nats.MaxWait(opts().Timeout))
			if err == nil {
				if progress != nil {
					state = "Handling  "
				}

				for _, msg := range msgs {
					mh(msg)
					i++
				}
			} else {
				if !c.progressBar {
					if err == nats.ErrTimeout {
						log.Print("Fetch timeout!")
					} else {
						errChan <- fmt.Errorf("fetching: %w", err)
						donewg.Done()
						return
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
	errChan <- nil
}
