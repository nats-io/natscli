// Copyright 2019-2025 The NATS Authors
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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/natscli/columns"
	"github.com/nats-io/natscli/internal/asciigraph"
	iu "github.com/nats-io/natscli/internal/util"
	terminal "golang.org/x/term"
	"gopkg.in/yaml.v3"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/balancer"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/jsm.go"
)

type consumerCmd struct {
	consumer       string
	stream         string
	json           bool
	listNames      bool
	force          bool
	ack            bool
	ackSetByUser   bool
	term           bool
	raw            bool
	destination    string
	inputFile      string
	outFile        string
	showAll        bool
	acceptDefaults bool
	showStateOnly  bool

	selectedConsumer *jsm.Consumer

	ackPolicy           string
	ackWait             time.Duration
	bpsRateLimit        uint64
	delivery            string
	ephemeral           bool
	filterSubjects      []string
	idleHeartbeat       string
	maxAckPending       int
	maxDeliver          int
	maxWaiting          int
	deliveryGroup       string
	pull                bool
	pullCount           int
	replayPolicy        string
	reportLeaderDistrib bool
	samplePct           int
	startPolicy         string
	validateOnly        bool
	description         string
	inactiveThreshold   time.Duration
	maxPullExpire       time.Duration
	maxPullBytes        int
	maxPullBatch        int
	backoffMode         string
	backoffSteps        uint
	backoffMin          time.Duration
	backoffMax          time.Duration
	replicas            int
	memory              bool
	hdrsOnly            bool
	hdrsOnlySet         bool
	fc                  bool
	fcSet               bool
	metadataIsSet       bool
	metadata            map[string]string
	pauseUntil          string

	dryRun             bool
	mgr                *jsm.Manager
	nc                 *nats.Conn
	nak                bool
	fPull              bool
	fPush              bool
	fBound             bool
	fWaiting           int
	fAckPending        int
	fPending           uint64
	fIdle              time.Duration
	fCreated           time.Duration
	fReplicas          uint
	fInvert            bool
	fExpression        string
	fLeader            string
	interactive        bool
	pinnedGroups       []string
	pinnedTTL          time.Duration
	overflowGroups     []string
	groupName          string
	fPinned            bool
	placementPreferred string
}

func configureConsumerCommand(app commandHost) {
	c := &consumerCmd{metadata: map[string]string{}}

	addCreateFlags := func(f *fisk.CmdClause, edit bool) {
		if !edit {
			f.Flag("ack", "Acknowledgment policy (none, all, explicit)").StringVar(&c.ackPolicy)
			f.Flag("bps", "Restrict message delivery to a certain bit per second").Default("0").Uint64Var(&c.bpsRateLimit)
		}
		f.Flag("backoff", "Creates a consumer backoff policy using a specific pre-written algorithm (none, linear)").PlaceHolder("MODE").EnumVar(&c.backoffMode, "linear", "none")
		f.Flag("backoff-steps", "Number of steps to use when creating the backoff policy").PlaceHolder("STEPS").Default("10").UintVar(&c.backoffSteps)
		f.Flag("backoff-min", "The shortest backoff period that will be generated").PlaceHolder("MIN").Default("1m").DurationVar(&c.backoffMin)
		f.Flag("backoff-max", "The longest backoff period that will be generated").PlaceHolder("MAX").Default("20m").DurationVar(&c.backoffMax)
		if !edit {
			f.Flag("deliver", "Start policy (all, new, last, subject, 1h, msg sequence)").PlaceHolder("POLICY").StringVar(&c.startPolicy)
			f.Flag("deliver-group", "Delivers push messages only to subscriptions matching this group").Default("_unset_").PlaceHolder("GROUP").StringVar(&c.deliveryGroup)
		}
		f.Flag("description", "Sets a contextual description for the consumer").StringVar(&c.description)
		if !edit {
			f.Flag("ephemeral", "Create an ephemeral Consumer").UnNegatableBoolVar(&c.ephemeral)
		}
		f.Flag("filter", "Filter Stream by subjects").PlaceHolder("SUBJECTS").StringsVar(&c.filterSubjects)
		if !edit {
			f.Flag("flow-control", "Enable Push consumer flow control").IsSetByUser(&c.fcSet).UnNegatableBoolVar(&c.fc)
			f.Flag("heartbeat", "Enable idle Push consumer heartbeats (-1 disable)").StringVar(&c.idleHeartbeat)
		}

		f.Flag("headers-only", "Deliver only headers and no bodies").IsSetByUser(&c.hdrsOnlySet).BoolVar(&c.hdrsOnly)
		f.Flag("max-deliver", "Maximum amount of times a message will be delivered").PlaceHolder("TRIES").IntVar(&c.maxDeliver)
		f.Flag("max-outstanding", "Maximum pending Acks before consumers stop delivering messages").Hidden().Default("-1").IntVar(&c.maxAckPending)
		f.Flag("max-pending", "Maximum pending Acks before consumers are paused").Default("-1").IntVar(&c.maxAckPending)
		if !edit {
			f.Flag("max-waiting", "Maximum number of outstanding pulls allowed").PlaceHolder("PULLS").IntVar(&c.maxWaiting)
		}
		f.Flag("max-pull-batch", "Maximum size batch size for a pull request to accept").PlaceHolder("BATCH_SIZE").IntVar(&c.maxPullBatch)
		f.Flag("max-pull-expire", "Maximum expire duration for a pull request to accept").PlaceHolder("EXPIRES").DurationVar(&c.maxPullExpire)
		f.Flag("max-pull-bytes", "Maximum max bytes for a pull request to accept").PlaceHolder("BYTES").IntVar(&c.maxPullBytes)
		if !edit {
			f.Flag("pull", "Deliver messages in 'pull' mode").UnNegatableBoolVar(&c.pull)
			f.Flag("replay", "Replay Policy (instant, original)").PlaceHolder("POLICY").EnumVar(&c.replayPolicy, "instant", "original")
		}
		f.Flag("sample", "Percentage of requests to sample for monitoring purposes").Default("-1").IntVar(&c.samplePct)
		f.Flag("target", "Push based delivery target subject").PlaceHolder("SUBJECT").StringVar(&c.delivery)
		f.Flag("wait", "Acknowledgment waiting time").Default("-1s").DurationVar(&c.ackWait)
		f.Flag("inactive-threshold", "How long to allow an ephemeral consumer to be idle before removing it").PlaceHolder("THRESHOLD").DurationVar(&c.inactiveThreshold)
		if !edit {
			f.Flag("memory", "Force the consumer state to be stored in memory rather than inherit from the stream").UnNegatableBoolVar(&c.memory)
		}
		f.Flag("replicas", "Sets a custom replica count rather than inherit from the stream").IntVar(&c.replicas)
		f.Flag("metadata", "Adds metadata to the consumer").PlaceHolder("META").IsSetByUser(&c.metadataIsSet).StringMapVar(&c.metadata)
		if !edit {
			f.Flag("pause", fmt.Sprintf("Pause the consumer for a duration after start or until a specific timestamp (eg %s)", time.Now().Format(time.DateTime))).StringVar(&c.pauseUntil)
			f.Flag("pinned-groups", "Create a Pinned Client consumer based on these groups").StringsVar(&c.pinnedGroups)
			f.Flag("pinned-ttl", "The time to allow for a client to pull before losing the pinned status").DurationVar(&c.pinnedTTL)
			f.Flag("overflow-groups", "Create a Overflow consumer based on these groups").StringsVar(&c.overflowGroups)
		}
	}

	cons := app.Command("consumer", "JetStream Consumer management").Alias("con").Alias("obs").Alias("c")
	addCheat("consumer", cons)
	cons.Flag("all", "Operate on all streams including system ones").Short('a').UnNegatableBoolVar(&c.showAll)

	consAdd := cons.Command("add", "Creates a new Consumer").Alias("create").Alias("new").Action(c.createAction)
	consAdd.Arg("stream", "Stream name").StringVar(&c.stream)
	consAdd.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consAdd.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	consAdd.Flag("validate", "Only validates the configuration against the official Schema").UnNegatableBoolVar(&c.validateOnly)
	consAdd.Flag("output", "Save configuration instead of creating").PlaceHolder("FILE").StringVar(&c.outFile)
	addCreateFlags(consAdd, false)
	consAdd.Flag("defaults", "Accept default values for all prompts").UnNegatableBoolVar(&c.acceptDefaults)

	edit := cons.Command("edit", "Edits the configuration of a consumer").Alias("update").Action(c.editAction)
	edit.Arg("stream", "Stream name").StringVar(&c.stream)
	edit.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	edit.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	edit.Flag("force", "Force removal without prompting").Short('f').UnNegatableBoolVar(&c.force)
	edit.Flag("interactive", "Edit the configuring using your editor").Short('i').BoolVar(&c.interactive)
	edit.Flag("dry-run", "Only shows differences, do not edit the stream").UnNegatableBoolVar(&c.dryRun)
	addCreateFlags(edit, true)

	consLs := cons.Command("ls", "List known Consumers").Alias("list").Action(c.lsAction)
	consLs.Arg("stream", "Stream name").StringVar(&c.stream)
	consLs.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	consLs.Flag("names", "Show just the consumer names").Short('n').UnNegatableBoolVar(&c.listNames)
	consLs.Flag("no-select", "Do not select consumers from a list").Default("false").UnNegatableBoolVar(&c.force)

	consFind := cons.Command("find", "Finds consumers matching certain criteria").Alias("query").Action(c.findAction)
	consFind.Arg("stream", "Stream name").StringVar(&c.stream)
	consFind.Flag("pull", "Display only pull based consumers").UnNegatableBoolVar(&c.fPull)
	consFind.Flag("push", "Display only push based consumers").UnNegatableBoolVar(&c.fPush)
	consFind.Flag("bound", "Display push-bound or pull consumers with waiting pulls").UnNegatableBoolVar(&c.fBound)
	consFind.Flag("waiting", "Display consumers with fewer waiting pulls").IntVar(&c.fWaiting)
	consFind.Flag("ack-pending", "Display consumers with fewer pending acks").IntVar(&c.fAckPending)
	consFind.Flag("pending", "Display consumers with fewer unprocessed messages").Uint64Var(&c.fPending)
	consFind.Flag("idle", "Display consumers with no new deliveries for a period").DurationVar(&c.fIdle)
	consFind.Flag("created", "Display consumers created longer ago than duration").PlaceHolder("DURATION").DurationVar(&c.fCreated)
	consFind.Flag("replicas", "Display consumers with fewer or equal replicas than the value").PlaceHolder("REPLICAS").UintVar(&c.fReplicas)
	consFind.Flag("leader", "Display only clustered streams with a specific leader").PlaceHolder("SERVER").StringVar(&c.fLeader)
	consFind.Flag("pinned", "Finds Pinned Client priority group consumers that are fully pinned").UnNegatableBoolVar(&c.fPinned)
	consFind.Flag("invert", "Invert the check - before becomes after, with becomes without").BoolVar(&c.fInvert)
	consFind.Flag("expression", "Match consumers using an expression language").StringVar(&c.fExpression)

	consInfo := cons.Command("info", "Consumer information").Alias("nfo").Action(c.infoAction)
	consInfo.Arg("stream", "Stream name").StringVar(&c.stream)
	consInfo.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consInfo.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	consInfo.Flag("no-select", "Do not select consumers from a list").Default("false").UnNegatableBoolVar(&c.force)

	consState := cons.Command("state", "Stream state").Action(c.stateAction)
	consState.Arg("stream", "Stream to retrieve state information for").StringVar(&c.stream)
	consState.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consState.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	consState.Flag("no-select", "Do not select streams from a list").Default("false").UnNegatableBoolVar(&c.force)

	consRm := cons.Command("rm", "Removes a Consumer").Alias("delete").Alias("del").Action(c.rmAction)
	consRm.Arg("stream", "Stream name").StringVar(&c.stream)
	consRm.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consRm.Flag("force", "Force removal without prompting").Short('f').UnNegatableBoolVar(&c.force)

	consCp := cons.Command("copy", "Creates a new Consumer based on the configuration of another").Alias("cp").Action(c.cpAction)
	consCp.Arg("stream", "Stream name").Required().StringVar(&c.stream)
	consCp.Arg("source", "Source Consumer name").Required().StringVar(&c.consumer)
	consCp.Arg("destination", "Destination Consumer name").Required().StringVar(&c.destination)
	addCreateFlags(consCp, false)

	consNext := cons.Command("next", "Retrieves messages from Pull Consumers without interactive prompts").Action(c.nextAction)
	consNext.Arg("stream", "Stream name").Required().StringVar(&c.stream)
	consNext.Arg("consumer", "Consumer name").Required().StringVar(&c.consumer)
	consNext.Flag("ack", "Acknowledge received message").Default("true").IsSetByUser(&c.ackSetByUser).BoolVar(&c.ack)
	consNext.Flag("nak", "Perform a Negative Acknowledgement on the message").UnNegatableBoolVar(&c.nak)
	consNext.Flag("term", "Terms the message").Default("false").UnNegatableBoolVar(&c.term)
	consNext.Flag("raw", "Show only the message").Short('r').UnNegatableBoolVar(&c.raw)
	consNext.Flag("wait", "Wait up to this period to acknowledge messages").DurationVar(&c.ackWait)
	consNext.Flag("count", "Number of messages to try to fetch from the pull consumer").Default("1").IntVar(&c.pullCount)

	consSub := cons.Command("sub", "Retrieves messages from Consumers").Action(c.subAction).Hidden()
	consSub.Arg("stream", "Stream name").StringVar(&c.stream)
	consSub.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	consSub.Flag("ack", "Acknowledge received message").Default("true").BoolVar(&c.ack)
	consSub.Flag("raw", "Show only the message").Short('r').UnNegatableBoolVar(&c.raw)
	consSub.Flag("deliver-group", "Deliver group of the consumer").StringVar(&c.deliveryGroup)

	graph := cons.Command("graph", "View a graph of Consumer activity").Action(c.graphAction)
	graph.Arg("stream", "Stream name").StringVar(&c.stream)
	graph.Arg("consumer", "Consumer name").StringVar(&c.consumer)

	conPause := cons.Command("pause", "Pause a consumer until a later time").Action(c.pauseAction)
	conPause.Arg("stream", "Stream name").StringVar(&c.stream)
	conPause.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	conPause.Arg("until", fmt.Sprintf("Pause until a specific time (eg %s)", time.Now().UTC().Format(time.DateTime))).PlaceHolder("TIME").StringVar(&c.pauseUntil)
	conPause.Flag("force", "Force pause without prompting").Short('f').UnNegatableBoolVar(&c.force)

	conUnpin := cons.Command("unpin", "Unpin the current Pinned Client from a Priority Group").Action(c.unpinAction)
	conUnpin.Arg("stream", "Stream name").StringVar(&c.stream)
	conUnpin.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	conUnpin.Arg("group", "The group to unpin").StringVar(&c.groupName)
	conUnpin.Flag("force", "Force unpin without prompting").Short('f').UnNegatableBoolVar(&c.force)

	conResume := cons.Command("resume", "Resume a paused consumer").Action(c.resumeAction)
	conResume.Arg("stream", "Stream name").StringVar(&c.stream)
	conResume.Arg("consumer", "Consumer name").StringVar(&c.consumer)
	conResume.Flag("force", "Force resume without prompting").Short('f').UnNegatableBoolVar(&c.force)

	conReport := cons.Command("report", "Reports on Consumer statistics").Action(c.reportAction)
	conReport.Arg("stream", "Stream name").StringVar(&c.stream)
	conReport.Flag("raw", "Show un-formatted numbers").Short('r').UnNegatableBoolVar(&c.raw)
	conReport.Flag("leaders", "Show details about the leaders").Short('l').UnNegatableBoolVar(&c.reportLeaderDistrib)

	conCluster := cons.Command("cluster", "Manages a clustered Consumer").Alias("c")
	conClusterDown := conCluster.Command("step-down", "Force a new leader election by standing down the current leader").Alias("elect").Alias("down").Alias("d").Action(c.leaderStandDownAction)
	conClusterDown.Arg("stream", "Stream to act on").StringVar(&c.stream)
	conClusterDown.Arg("consumer", "Consumer to act on").StringVar(&c.consumer)
	conClusterDown.Flag("preferred", "Prefer placing the leader on a specific host").StringVar(&c.placementPreferred)
	conClusterDown.Flag("force", "Force leader step down ignoring current leader").Short('f').UnNegatableBoolVar(&c.force)

	conClusterBalance := conCluster.Command("balance", "Balance consumer leaders").Action(c.balanceAction)
	conClusterBalance.Arg("stream", "Stream to act on").StringVar(&c.stream)
	conClusterBalance.Flag("pull", "Balance only pull based consumers").UnNegatableBoolVar(&c.fPull)
	conClusterBalance.Flag("push", "Balance only push based consumers").UnNegatableBoolVar(&c.fPush)
	conClusterBalance.Flag("bound", "Balance push-bound or pull consumers with waiting pulls").UnNegatableBoolVar(&c.fBound)
	conClusterBalance.Flag("waiting", "Balance consumers with fewer waiting pulls").IntVar(&c.fWaiting)
	conClusterBalance.Flag("ack-pending", "Balance consumers with fewer pending acks").IntVar(&c.fAckPending)
	conClusterBalance.Flag("pending", "Balance consumers with fewer unprocessed messages").Uint64Var(&c.fPending)
	conClusterBalance.Flag("idle", "Balance consumers with no new deliveries for a period").DurationVar(&c.fIdle)
	conClusterBalance.Flag("created", "Balance consumers created longer ago than duration").PlaceHolder("DURATION").DurationVar(&c.fCreated)
	conClusterBalance.Flag("replicas", "Balance consumers with fewer or equal replicas than the value").PlaceHolder("REPLICAS").UintVar(&c.fReplicas)
	conClusterBalance.Flag("leader", "Balance only clustered streams with a specific leader").PlaceHolder("SERVER").StringVar(&c.fLeader)
	conClusterBalance.Flag("pinned", "Balance Pinned Client priority group consumers that are fully pinned").UnNegatableBoolVar(&c.fPinned)
	conClusterBalance.Flag("invert", "Invert the check - before becomes after, with becomes without").BoolVar(&c.fInvert)
	conClusterBalance.Flag("expression", "Balance matching consumers using an expression language").StringVar(&c.fExpression)

}

func init() {
	registerCommand("consumer", 4, configureConsumerCommand)
}

func (c *consumerCmd) unpinAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(true, true)

	if !c.selectedConsumer.IsPinnedClientPriority() {
		return fmt.Errorf("consumer is not a pinned priority consumer")
	}

	nfo, err := c.selectedConsumer.State()
	if err != nil {
		return err
	}

	matched := map[string]api.PriorityGroupState{}
	var groups []string
	for _, v := range nfo.PriorityGroups {
		if v.PinnedClientID != "" {
			matched[v.Group] = v
			groups = append(groups, v.Group)
		}
	}

	if len(matched) == 0 {
		return fmt.Errorf("no priority groups have pinned clients")
	}

	if c.groupName == "" {
		err = iu.AskOne(&survey.Select{
			Message:  "Select a Group",
			Options:  groups,
			PageSize: iu.SelectPageSize(len(groups)),
		}, &c.groupName, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really unpin client from group %s > %s > %s", c.stream, c.consumer, c.groupName), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = c.selectedConsumer.Unpin(c.groupName)
	if err != nil {
		return err
	}

	fmt.Printf("Unpinned client %s from Priority Group %s > %s > %s\n", matched[c.groupName].PinnedClientID, c.stream, c.consumer, c.groupName)

	return nil
}

func (c *consumerCmd) findAction(_ *fisk.ParseContext) error {
	var err error
	var stream *jsm.Stream

	c.connectAndSetup(true, false)

	c.stream, stream, err = selectStream(c.mgr, c.stream, c.force, c.showAll)
	if err != nil {
		return err
	}

	if stream == nil {
		return fmt.Errorf("no stream selected")
	}

	var opts []jsm.ConsumerQueryOpt
	if c.fPush {
		opts = append(opts, jsm.ConsumerQueryIsPush())
	}
	if c.fPull {
		opts = append(opts, jsm.ConsumerQueryIsPull())
	}
	if c.fBound {
		opts = append(opts, jsm.ConsumerQueryIsBound())
	}
	if c.fWaiting > 0 {
		opts = append(opts, jsm.ConsumerQueryWithFewerWaiting(c.fWaiting))
	}
	if c.fAckPending > 0 {
		opts = append(opts, jsm.ConsumerQueryWithFewerAckPending(c.fAckPending))
	}
	if c.fPending > 0 {
		opts = append(opts, jsm.ConsumerQueryWithFewerPending(c.fPending))
	}
	if c.fIdle > 0 {
		opts = append(opts, jsm.ConsumerQueryWithDeliverySince(c.fIdle))
	}
	if c.fCreated > 0 {
		opts = append(opts, jsm.ConsumerQueryOlderThan(c.fCreated))
	}
	if c.fReplicas > 0 {
		opts = append(opts, jsm.ConsumerQueryReplicas(c.fReplicas))
	}
	if c.fInvert {
		opts = append(opts, jsm.ConsumerQueryInvert())
	}
	if c.fExpression != "" {
		opts = append(opts, jsm.ConsumerQueryExpression(c.fExpression))
	}
	if c.fLeader != "" {
		opts = append(opts, jsm.ConsumerQueryLeaderServer(c.fLeader))
	}
	if c.fPinned {
		opts = append(opts, jsm.ConsumerQueryIsPinned())
	}

	found, err := stream.QueryConsumers(opts...)
	if err != nil {
		return err
	}

	for _, c := range found {
		fmt.Println(c.Name())
	}

	return nil
}

func (c *consumerCmd) graphAction(_ *fisk.ParseContext) error {
	if !iu.IsTerminal() {
		return fmt.Errorf("can only graph data on an interactive terminal")
	}

	width, height, err := terminal.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return fmt.Errorf("failed to get terminal dimensions: %w", err)
	}

	if width < 20 || height < 20 {
		return fmt.Errorf("please increase terminal dimensions")
	}

	c.connectAndSetup(true, true)

	consumer, err := c.mgr.LoadConsumer(c.stream, c.consumer)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	nfo, err := consumer.State()
	if err != nil {
		return err
	}

	resizeData := func(data []float64, width int) []float64 {
		if width <= 0 {
			return data
		}

		length := len(data)

		if length > width {
			return data[length-width:]
		}

		return data
	}

	deliveredRates := make([]float64, width)
	ackedRates := make([]float64, width)
	outstandingMessages := make([]float64, width)
	unprocessedMessages := make([]float64, width)
	lastAckedSeq := nfo.AckFloor.Stream
	lastDeliveredSeq := nfo.Delivered.Stream
	lastStateTs := time.Now()

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			width, height, err = terminal.GetSize(int(os.Stdout.Fd()))
			if err != nil {
				height = 40
				width = 80
			}
			if width > 15 {
				width -= 10
			}
			if height > 10 {
				height -= 5
			}

			if width < 20 || height < 20 {
				return fmt.Errorf("please increase terminal dimensions")
			}

			nfo, err := consumer.State()
			if err != nil {
				continue
			}

			deliveredRates = append(deliveredRates, calculateRate(float64(nfo.Delivered.Stream), float64(lastDeliveredSeq), time.Since(lastStateTs)))
			ackedRates = append(ackedRates, calculateRate(float64(nfo.AckFloor.Stream), float64(lastAckedSeq), time.Since(lastStateTs)))
			unprocessedMessages = append(unprocessedMessages, float64(nfo.NumPending))
			outstandingMessages = append(outstandingMessages, float64(nfo.NumAckPending))
			lastDeliveredSeq = nfo.Delivered.Stream
			lastAckedSeq = nfo.AckFloor.Stream
			lastStateTs = time.Now()

			deliveredRates = resizeData(deliveredRates, width)
			ackedRates = resizeData(ackedRates, width)
			unprocessedMessages = resizeData(unprocessedMessages, width)
			outstandingMessages = resizeData(outstandingMessages, width)

			deliveredPlot := asciigraph.Plot(deliveredRates,
				asciigraph.Caption("Messages Delivered / second"),
				asciigraph.Width(width),
				asciigraph.Height(height/4-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(f),
			)

			ackedPlot := asciigraph.Plot(ackedRates,
				asciigraph.Caption("Messages Acknowledged / second"),
				asciigraph.Width(width),
				asciigraph.Height(height/4-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(f),
			)

			unprocessedPlot := asciigraph.Plot(unprocessedMessages,
				asciigraph.Caption("Messages Pending"),
				asciigraph.Width(width),
				asciigraph.Height(height/4-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(fFloat2Int),
			)

			outstandingPlot := asciigraph.Plot(outstandingMessages,
				asciigraph.Caption("Messages Waiting for Ack"),
				asciigraph.Width(width),
				asciigraph.Height(height/4-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(fFloat2Int),
			)

			iu.ClearScreen()

			fmt.Printf("Consumer Statistics for %s > %s\n", c.stream, c.consumer)
			fmt.Println()
			fmt.Println(unprocessedPlot)
			fmt.Println()
			fmt.Println(outstandingPlot)
			fmt.Println()
			fmt.Println(ackedPlot)
			fmt.Println()
			fmt.Println(deliveredPlot)

		case <-ctx.Done():
			iu.ClearScreen()
			return nil
		}
	}
}

func (c *consumerCmd) balanceAction(_ *fisk.ParseContext) error {
	var err error
	var stream *jsm.Stream

	c.connectAndSetup(true, false)

	c.stream, stream, err = selectStream(c.mgr, c.stream, c.force, c.showAll)
	if err != nil {
		return err
	}

	if stream == nil {
		return fmt.Errorf("no stream selected")
	}

	var opts []jsm.ConsumerQueryOpt
	if c.fPush {
		opts = append(opts, jsm.ConsumerQueryIsPush())
	}
	if c.fPull {
		opts = append(opts, jsm.ConsumerQueryIsPull())
	}
	if c.fBound {
		opts = append(opts, jsm.ConsumerQueryIsBound())
	}
	if c.fWaiting > 0 {
		opts = append(opts, jsm.ConsumerQueryWithFewerWaiting(c.fWaiting))
	}
	if c.fAckPending > 0 {
		opts = append(opts, jsm.ConsumerQueryWithFewerAckPending(c.fAckPending))
	}
	if c.fPending > 0 {
		opts = append(opts, jsm.ConsumerQueryWithFewerPending(c.fPending))
	}
	if c.fIdle > 0 {
		opts = append(opts, jsm.ConsumerQueryWithDeliverySince(c.fIdle))
	}
	if c.fCreated > 0 {
		opts = append(opts, jsm.ConsumerQueryOlderThan(c.fCreated))
	}
	if c.fReplicas > 0 {
		opts = append(opts, jsm.ConsumerQueryReplicas(c.fReplicas))
	}
	if c.fInvert {
		opts = append(opts, jsm.ConsumerQueryInvert())
	}
	if c.fExpression != "" {
		opts = append(opts, jsm.ConsumerQueryExpression(c.fExpression))
	}
	if c.fLeader != "" {
		opts = append(opts, jsm.ConsumerQueryLeaderServer(c.fLeader))
	}
	if c.fPinned {
		opts = append(opts, jsm.ConsumerQueryIsPinned())
	}

	consumers, err := stream.QueryConsumers(opts...)
	if err != nil {
		return err
	}

	if len(consumers) > 0 {
		balancer, err := balancer.New(c.mgr.NatsConn(), api.NewDefaultLogger(api.InfoLevel))
		if err != nil {
			return err
		}

		balanced, err := balancer.BalanceConsumers(consumers)
		if err != nil {
			return fmt.Errorf("failed to balance consumers on %s - %s", c.stream, err)
		}
		fmt.Printf("Balanced %d consumers on %s\n", balanced, c.stream)

	} else {
		fmt.Printf("No consumers on %s\n", c.stream)
	}
	return nil
}

func (c *consumerCmd) leaderStandDownAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(true, true)

	consumer, err := c.mgr.LoadConsumer(c.stream, c.consumer)
	if err != nil {
		return err
	}

	info, err := consumer.LatestState()
	if err != nil {
		return err
	}

	if info.Cluster == nil {
		return fmt.Errorf("consumer %q > %q is not clustered", consumer.StreamName(), consumer.Name())
	}

	leader := info.Cluster.Leader
	if leader == "" && !c.force {
		return fmt.Errorf("consumer has no current leader")
	} else if leader == "" {
		leader = "<unknown>"
	}

	var p *api.Placement
	if c.placementPreferred != "" {
		err = iu.RequireAPILevel(c.mgr, 1, "placement hints during step-down requires NATS Server 2.11")
		if err != nil {
			return err
		}

		p = &api.Placement{Preferred: c.placementPreferred}
	}

	log.Printf("Requesting leader step down of %q in a %d peer RAFT group", leader, len(info.Cluster.Replicas)+1)
	err = consumer.LeaderStepDown(p)
	if err != nil {
		return err
	}

	ctr := 0
	start := time.Now()
	for range time.NewTimer(500 * time.Millisecond).C {
		if ctr == 5 {
			return fmt.Errorf("consumer did not elect a new leader in time")
		}
		ctr++

		info, err = consumer.State()
		if err != nil {
			log.Printf("Failed to retrieve Consumer State: %s", err)
			continue
		}

		if info.Cluster == nil {
			log.Printf("Failed to retrieve Consumer State: no cluster information received")
			continue
		}

		if info.Cluster.Leader != leader {
			log.Printf("New leader elected %q", info.Cluster.Leader)
			break
		}
	}

	switch {
	case info.Cluster == nil:
		log.Printf("Consumer did not elect a leader after %s", time.Since(start).Round(time.Millisecond))
	case info.Cluster.Leader == leader:
		log.Printf("Leader did not change after %s", time.Since(start).Round(time.Millisecond))
	}

	fmt.Println()
	c.showConsumer(consumer)
	return nil
}

func (c *consumerCmd) interactiveEdit(cfg api.ConsumerConfig) (*api.ConsumerConfig, error) {
	cj, err := decoratedYamlMarshal(cfg)
	if err != nil {
		return &api.ConsumerConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile, err := os.CreateTemp("", "*.yaml")
	if err != nil {
		return &api.ConsumerConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}
	defer os.Remove(tfile.Name())

	_, err = fmt.Fprint(tfile, string(cj))
	if err != nil {
		return &api.ConsumerConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile.Close()

	err = iu.EditFile(tfile.Name())
	if err != nil {
		return &api.ConsumerConfig{}, err
	}

	nb, err := os.ReadFile(tfile.Name())
	if err != nil {
		return &api.ConsumerConfig{}, err
	}

	ncfg := api.ConsumerConfig{}
	err = yaml.Unmarshal(nb, &ncfg)
	if err != nil {
		return &api.ConsumerConfig{}, err
	}

	// some yaml quirks
	if len(ncfg.BackOff) == 0 {
		ncfg.BackOff = nil
	}

	return &ncfg, nil
}

func (c *consumerCmd) copyAndEditConsumer(cfg api.ConsumerConfig) (*api.ConsumerConfig, error) {
	var err error

	if c.inputFile != "" {
		return c.loadConfigFile(c.inputFile)
	}

	if c.description != "" {
		cfg.Description = c.description
	}

	if c.inactiveThreshold != 0 {
		cfg.InactiveThreshold = c.inactiveThreshold
	}

	if c.maxDeliver != 0 {
		cfg.MaxDeliver = c.maxDeliver
	}

	if c.maxAckPending != -1 {
		cfg.MaxAckPending = c.maxAckPending
	}

	if c.ackWait != -1*time.Second {
		cfg.AckWait = c.ackWait
	}

	if c.maxWaiting != 0 {
		cfg.MaxWaiting = c.maxWaiting
	}

	if c.samplePct != -1 {
		cfg.SampleFrequency = c.sampleFreqFromInt(c.samplePct)
	}

	if c.maxPullBatch > 0 {
		cfg.MaxRequestBatch = c.maxPullBatch
	}

	if c.maxPullExpire > 0 {
		cfg.MaxRequestExpires = c.maxPullExpire
	}

	if c.maxPullBytes > 0 {
		cfg.MaxRequestMaxBytes = c.maxPullBytes
	}

	if c.backoffMode != "" {
		cfg.BackOff, err = c.backoffPolicy()
		if err != nil {
			return &api.ConsumerConfig{}, fmt.Errorf("could not determine backoff policy: %v", err)
		}
	}

	if c.delivery != "" {
		cfg.DeliverSubject = c.delivery
	}

	if c.hdrsOnlySet {
		cfg.HeadersOnly = c.hdrsOnly
	}

	if len(c.filterSubjects) == 1 {
		cfg.FilterSubject = c.filterSubjects[0]
		cfg.FilterSubjects = nil
	} else if len(c.filterSubjects) > 1 {
		cfg.FilterSubjects = c.filterSubjects
		cfg.FilterSubject = ""
	}

	if c.replicas > 0 {
		cfg.Replicas = c.replicas
	}

	if c.metadataIsSet {
		cfg.Metadata = c.metadata
	}

	return &cfg, nil
}
func (c *consumerCmd) editAction(pc *fisk.ParseContext) error {
	c.connectAndSetup(true, true)
	var err error

	if c.selectedConsumer == nil {
		c.selectedConsumer, err = c.mgr.LoadConsumer(c.stream, c.consumer)
		fisk.FatalIfError(err, "could not load Consumer")
	}

	if !c.selectedConsumer.IsDurable() {
		return fmt.Errorf("only durable consumers can be edited")
	}

	// lazy deep copy
	t := c.selectedConsumer.Configuration()
	t.Metadata = iu.RemoveReservedMetadata(t.Metadata)

	tj, err := json.Marshal(t)
	if err != nil {
		return err
	}

	var ncfg *api.ConsumerConfig
	err = json.Unmarshal(tj, &ncfg)
	if err != nil {
		return err
	}

	if c.interactive {
		ncfg, err = c.interactiveEdit(t)
		fisk.FatalIfError(err, "could not create new configuration for Consumer %s", c.selectedConsumer.Name())
	} else {
		ncfg, err = c.copyAndEditConsumer(t)
		fisk.FatalIfError(err, "could not create new configuration for Consumer %s", c.selectedConsumer.Name())
	}

	if len(ncfg.BackOff) > 0 && ncfg.AckWait != t.AckWait {
		return fmt.Errorf("consumers with backoff policies do not support editing Ack Wait")
	}

	// sort strings to subject lists that only differ in ordering is considered equal
	sorter := cmp.Transformer("Sort", func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	})

	t.Metadata = iu.RemoveReservedMetadata(t.Metadata)
	ncfg.Metadata = iu.RemoveReservedMetadata(ncfg.Metadata)

	diff := cmp.Diff(t, *ncfg, sorter)
	if diff == "" {
		if !c.dryRun {
			fmt.Println("No difference in configuration")
		}

		return nil
	}

	fmt.Printf("Differences (-old +new):\n%s", diff)
	if c.dryRun {
		os.Exit(1)
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really edit Consumer %s > %s", c.stream, c.consumer), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = c.checkConfigLevel(ncfg)
	if err != nil {
		return err
	}

	cons, err := c.mgr.NewConsumerFromDefault(c.stream, *ncfg)
	if err != nil {
		return err
	}

	c.showConsumer(cons)

	return nil
}

func (c *consumerCmd) backoffPolicy() ([]time.Duration, error) {
	if c.backoffMode == "none" {
		return nil, nil
	}

	if c.backoffMode == "" || c.backoffSteps == 0 || c.backoffMin == 0 || c.backoffMax == 0 {
		return nil, fmt.Errorf("required policy properties not supplied")
	}

	switch c.backoffMode {
	case "linear":
		return jsm.LinearBackoffPeriods(c.backoffSteps, c.backoffMin, c.backoffMax)
	default:
		return nil, fmt.Errorf("invalid backoff mode %q", c.backoffMode)
	}
}

func (c *consumerCmd) rmAction(_ *fisk.ParseContext) error {
	var err error

	if c.force {
		if c.stream == "" || c.consumer == "" {
			return fmt.Errorf("--force requires a stream and consumer name")
		}

		c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
		fisk.FatalIfError(err, "setup failed")

		err = c.mgr.DeleteConsumer(c.stream, c.consumer)
		if err != nil {
			if err == context.DeadlineExceeded {
				fmt.Println("Delete failed due to timeout, the stream or consumer might not exist or be in an unmanageable state")
			}
		}

		return err
	}

	c.connectAndSetup(true, true)

	ok, err := askConfirmation(fmt.Sprintf("Really delete Consumer %s > %s", c.stream, c.consumer), false)
	fisk.FatalIfError(err, "could not obtain confirmation")

	if !ok {
		return nil
	}

	if c.selectedConsumer == nil {
		c.selectedConsumer, err = c.mgr.LoadConsumer(c.stream, c.consumer)
		fisk.FatalIfError(err, "could not load Consumer")
	}

	return c.selectedConsumer.Delete()
}

func (c *consumerCmd) lsAction(pc *fisk.ParseContext) error {
	c.connectAndSetup(true, false)

	stream, err := c.mgr.LoadStream(c.stream)
	fisk.FatalIfError(err, "could not load Consumers")

	consumerNames, err := stream.ConsumerNames()
	fisk.FatalIfError(err, "could not load Consumers")

	if c.json {
		err = iu.PrintJSON(consumerNames)
		fisk.FatalIfError(err, "could not display Consumers")
		return nil
	}

	if c.listNames {
		for _, sc := range consumerNames {
			fmt.Println(sc)
		}

		return nil
	}

	if len(consumerNames) == 0 {
		fmt.Println("No Consumers defined")
		return nil
	}

	out, err := c.renderConsumerAsTable(stream)
	if err != nil {
		return err
	}

	fmt.Println(out)

	return nil
}

func (c *consumerCmd) renderConsumerAsTable(stream *jsm.Stream) (string, error) {
	var out bytes.Buffer
	table := iu.NewTableWriter(opts(), "Consumers")
	table.AddHeaders("Name", "Description", "Created", "Ack Pending", "Unprocessed", "Last Delivery")

	missing, err := stream.EachConsumer(func(cons *jsm.Consumer) {
		cs, err := cons.LatestState()
		if err != nil {
			log.Printf("Could not obtain consumer state for %s: %s", cons.Name(), err)
			return
		}

		lastDelivery := sinceRefOrNow(cs.TimeStamp, time.Time{})
		if cs.Delivered.Last != nil {
			lastDelivery = sinceRefOrNow(cs.TimeStamp, *cs.Delivered.Last)
		}

		table.AddRow(cs.Name, cs.Config.Description, f(cs.Created.Local()), cs.NumAckPending, cs.NumPending, f(lastDelivery))

	})
	if err != nil {
		return "", err
	}

	fmt.Fprintln(&out, table.Render())

	if len(missing) > 0 {
		c.renderMissing(&out, missing)
	}

	return out.String(), nil
}

func (c *consumerCmd) showConsumer(consumer *jsm.Consumer) {
	config := consumer.Configuration()
	state, err := consumer.LatestState()
	fisk.FatalIfError(err, "could not load Consumer %s > %s", c.stream, c.consumer)

	c.showInfo(config, state)
}

func (c *consumerCmd) renderBackoff(bo []time.Duration) string {
	if len(bo) == 0 {
		return "unset"
	}

	var times []string

	if len(bo) > 15 {
		for _, d := range bo[:5] {
			times = append(times, d.String())
		}
		times = append(times, "...")
		for _, d := range bo[len(bo)-5:] {
			times = append(times, d.String())
		}

		return fmt.Sprintf("%s (%d total)", f(times), len(bo))
	} else {
		for _, p := range bo {
			times = append(times, p.String())
		}

		return f(times)
	}
}

func (c *consumerCmd) showInfo(config api.ConsumerConfig, state api.ConsumerInfo) {
	if c.json {
		iu.PrintJSON(state)
		return
	}

	var cols *columns.Writer
	if c.showStateOnly {
		cols = newColumns(fmt.Sprintf("State for Consumer %s > %s created %s", state.Stream, state.Name, state.Created.Local().Format(time.RFC3339)))
	} else {
		cols = newColumns(fmt.Sprintf("Information for Consumer %s > %s created %s", state.Stream, state.Name, state.Created.Local().Format(time.RFC3339)))
	}

	if !c.showStateOnly {
		cols.AddSectionTitle("Configuration")

		cols.AddRowIfNotEmpty("Name", config.Name)
		cols.AddRowIf("Durable Name", config.Durable, config.Durable != "" && config.Durable != config.Name)
		cols.AddRowIfNotEmpty("Description", config.Description)

		if config.DeliverSubject != "" {
			cols.AddRow("Delivery Subject", config.DeliverSubject)
		} else {
			cols.AddRow("Pull Mode", true)
		}

		if config.FilterSubject != "" {
			cols.AddRow("Filter Subject", config.FilterSubject)
		} else if len(config.FilterSubjects) > 0 {
			cols.AddRow("Filter Subjects", config.FilterSubjects)
		}

		switch config.DeliverPolicy {
		case api.DeliverAll:
			cols.AddRow("Deliver Policy", "All")
		case api.DeliverLast:
			cols.AddRow("Deliver Policy", "Last")
		case api.DeliverNew:
			cols.AddRow("Deliver Policy", "New")
		case api.DeliverLastPerSubject:
			cols.AddRow("Deliver Policy", "Last Per Subject")
		case api.DeliverByStartTime:
			cols.AddRowf("Deliver Policy", "Since %s", config.OptStartTime)
		case api.DeliverByStartSequence:
			cols.AddRowf("Deliver Policy", "From Sequence %d", config.OptStartSeq)
		}

		cols.AddRowIf("Deliver Queue Group", config.DeliverGroup, config.DeliverGroup != "" && config.DeliverSubject != "")
		cols.AddRow("Ack Policy", config.AckPolicy.String())
		cols.AddRowIf("Ack Wait", config.AckWait, config.AckPolicy != api.AckNone)
		cols.AddRow("Replay Policy", config.ReplayPolicy.String())
		cols.AddRowIf("Maximum Deliveries", config.MaxDeliver, config.MaxDeliver != -1)
		cols.AddRowIfNotEmpty("Sampling Rate", config.SampleFrequency)
		cols.AddRowIf("Rate Limit", fmt.Sprintf("%s / second", humanize.IBytes(config.RateLimit/8)), config.RateLimit > 0)
		cols.AddRowIf("Max Ack Pending", config.MaxAckPending, config.MaxAckPending > 0)
		cols.AddRowIf("Max Waiting Pulls", int64(config.MaxWaiting), config.MaxWaiting > 0)
		cols.AddRowIf("Idle Heartbeat", config.Heartbeat, config.Heartbeat > 0)
		cols.AddRowIf("Flow Control", config.FlowControl, config.DeliverSubject != "")
		cols.AddRowIf("Headers Only", true, config.HeadersOnly)
		cols.AddRowIf("Inactive Threshold", config.InactiveThreshold, config.InactiveThreshold > 0 && config.DeliverSubject == "")
		cols.AddRowIf("Max Pull Expire", config.MaxRequestExpires, config.MaxRequestExpires > 0)
		cols.AddRowIf("Max Pull Batch", config.MaxRequestBatch, config.MaxRequestBatch > 0)
		cols.AddRowIf("Max Pull MaxBytes", config.MaxRequestMaxBytes, config.MaxRequestMaxBytes > 0)
		cols.AddRowIf("Backoff", c.renderBackoff(config.BackOff), len(config.BackOff) > 0)
		cols.AddRowIf("Replicas", config.Replicas, config.Replicas > 0)
		cols.AddRowIf("Memory Storage", true, config.MemoryStorage)
		if state.Paused {
			cols.AddRowf("Paused Until Deadline", "%s (%s remaining)", f(config.PauseUntil), state.PauseRemaining.Round(time.Second))
		} else {
			cols.AddRowIf("Paused Until Deadline", fmt.Sprintf("%s (passed)", f(config.PauseUntil)), !config.PauseUntil.IsZero())
		}
		if config.PriorityPolicy != api.PriorityNone {
			cols.AddRow("Priority Policy", config.PriorityPolicy)
			cols.AddRow("Priority Groups", config.PriorityGroups)
			cols.AddRowIf("Pinned TTL", config.PinnedTTL, config.PriorityPolicy == api.PriorityPinnedClient)
		}

		meta := iu.RemoveReservedMetadata(config.Metadata)
		if len(meta) > 0 {
			cols.AddSectionTitle("Metadata")
			cols.AddMapStrings(meta)
		}
	}

	if state.Cluster != nil && state.Cluster.Name != "" {
		cols.AddSectionTitle("Cluster Information")
		cols.AddRow("Name", state.Cluster.Name)
		cols.AddRowIfNotEmpty("Raft Group", state.Cluster.RaftGroup)
		cols.AddRow("Leader", state.Cluster.Leader)
		for _, r := range state.Cluster.Replicas {
			since := fmt.Sprintf("seen %s ago", f(r.Active))
			if r.Active == 0 || r.Active == math.MaxInt64 {
				since = "not seen"
			}

			if r.Current {
				cols.AddRowf("Replica", "%s, current, %s", r.Name, since)
			} else {
				cols.AddRowf("Replica", "%s, outdated, %s", r.Name, since)
			}
		}
	}

	cols.AddSectionTitle("State")
	iu.RenderMetaApi(cols, config.Metadata)
	if state.Delivered.Last == nil {
		cols.AddRowf("Last Delivered Message", "Consumer sequence: %s Stream sequence: %s", f(state.Delivered.Consumer), f(state.Delivered.Stream))
	} else {
		cols.AddRowf("Last Delivered Message", "Consumer sequence: %s Stream sequence: %s Last delivery: %s ago", f(state.Delivered.Consumer), f(state.Delivered.Stream), f(sinceRefOrNow(state.TimeStamp, *state.Delivered.Last)))
	}

	if config.AckPolicy != api.AckNone {
		if state.AckFloor.Last == nil {
			cols.AddRowf("Acknowledgment Floor", "Consumer sequence: %s Stream sequence: %s", f(state.AckFloor.Consumer), f(state.AckFloor.Stream))
		} else {
			cols.AddRowf("Acknowledgment Floor", "Consumer sequence: %s Stream sequence: %s Last Ack: %s ago", f(state.AckFloor.Consumer), f(state.AckFloor.Stream), f(sinceRefOrNow(state.TimeStamp, *state.AckFloor.Last)))
		}
		if config.MaxAckPending > 0 {
			cols.AddRowf("Outstanding Acks", "%s out of maximum %s", f(state.NumAckPending), f(config.MaxAckPending))
		} else {
			cols.AddRow("Outstanding Acks", state.NumAckPending)
		}
		cols.AddRow("Redelivered Messages", state.NumRedelivered)
	}

	cols.AddRow("Unprocessed Messages", state.NumPending)

	if config.DeliverSubject == "" {
		if config.MaxWaiting > 0 {
			cols.AddRowf("Waiting Pulls", "%s of maximum %s", f(state.NumWaiting), f(config.MaxWaiting))
		} else {
			cols.AddRowf("Waiting Pulls", "%s of unlimited", f(state.NumWaiting))
		}
	} else {
		if state.PushBound {
			if config.DeliverGroup != "" {
				cols.AddRowf("Active Interest", "Active using Queue Group %s", config.DeliverGroup)
			} else {
				cols.AddRow("Active Interest", "Active")
			}
		} else {
			cols.AddRow("Active Interest", "No interest")
		}
	}
	if state.Paused {
		cols.AddRowf("Paused Until", "%s (%s remaining)", f(state.TimeStamp.Add(state.PauseRemaining)), state.PauseRemaining.Round(time.Second))
	}

	if len(state.PriorityGroups) > 0 && config.PriorityPolicy == api.PriorityPinnedClient {
		groups := map[string]string{}
		for _, v := range state.PriorityGroups {
			msg := "No client"
			if v.PinnedClientID != "" {
				msg = fmt.Sprintf("pinned %s at %s", v.PinnedClientID, f(v.PinnedTS))
			}

			groups[v.Group] = msg
		}
		cols.AddMapStringsAsValue("Priority Groups", groups)
	}

	cols.Frender(os.Stdout)
}

func (c *consumerCmd) stateAction(pc *fisk.ParseContext) error {
	c.showStateOnly = true
	return c.infoAction(pc)
}

func (c *consumerCmd) infoAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(true, true)

	var err error
	consumer := c.selectedConsumer

	if consumer == nil {
		consumer, err = c.mgr.LoadConsumer(c.stream, c.consumer)
		fisk.FatalIfError(err, "could not load Consumer %s > %s", c.stream, c.consumer)
	}

	c.showConsumer(consumer)

	return nil
}

func (c *consumerCmd) replayPolicyFromString(p string) api.ReplayPolicy {
	switch strings.ToLower(p) {
	case "instant":
		return api.ReplayInstant
	case "original":
		return api.ReplayOriginal
	default:
		fisk.Fatalf("invalid replay policy '%s'", p)
		return api.ReplayInstant
	}
}

func (c *consumerCmd) ackPolicyFromString(p string) api.AckPolicy {
	switch strings.ToLower(p) {
	case "none":
		return api.AckNone
	case "all":
		return api.AckAll
	case "explicit":
		return api.AckExplicit
	default:
		fisk.Fatalf("invalid ack policy '%s'", p)
		// unreachable
		return api.AckExplicit
	}
}

func (c *consumerCmd) sampleFreqFromInt(s int) string {
	if s > 100 || s < 0 {
		fisk.Fatalf("sample percent is not between 0 and 100")
	}

	if s > 0 {
		return strconv.Itoa(c.samplePct)
	}

	return ""
}

func (c *consumerCmd) defaultConsumer() *api.ConsumerConfig {
	return &api.ConsumerConfig{
		AckPolicy:    api.AckExplicit,
		ReplayPolicy: api.ReplayInstant,
	}
}

func (c *consumerCmd) setStartPolicy(cfg *api.ConsumerConfig, policy string) {
	if policy == "" {
		return
	}

	if policy == "all" {
		cfg.DeliverPolicy = api.DeliverAll
	} else if policy == "last" {
		cfg.DeliverPolicy = api.DeliverLast
	} else if policy == "new" || policy == "next" {
		cfg.DeliverPolicy = api.DeliverNew
	} else if policy == "subject" || policy == "last_per_subject" {
		cfg.DeliverPolicy = api.DeliverLastPerSubject
	} else if ok, _ := regexp.MatchString("^\\d+$", policy); ok {
		seq, _ := strconv.Atoi(policy)
		cfg.DeliverPolicy = api.DeliverByStartSequence
		cfg.OptStartSeq = uint64(seq)
	} else {
		d, err := fisk.ParseDuration(policy)
		fisk.FatalIfError(err, "could not parse starting delta")
		t := time.Now().UTC().Add(-d)
		cfg.DeliverPolicy = api.DeliverByStartTime
		cfg.OptStartTime = &t
	}
}

func (c *consumerCmd) cpAction(pc *fisk.ParseContext) (err error) {
	c.connectAndSetup(true, false)

	source, err := c.mgr.LoadConsumer(c.stream, c.consumer)
	fisk.FatalIfError(err, "could not load source Consumer")

	cfg := source.Configuration()

	if c.ackWait > 0 {
		cfg.AckWait = c.ackWait
	}

	if c.samplePct != -1 {
		cfg.SampleFrequency = c.sampleFreqFromInt(c.samplePct)
	}

	if c.startPolicy != "" {
		c.setStartPolicy(&cfg, c.startPolicy)
	}

	if c.ephemeral {
		cfg.Durable = ""
	} else {
		cfg.Durable = c.destination
	}

	if c.delivery != "" {
		cfg.DeliverSubject = c.delivery
	}

	if c.pull {
		cfg.DeliverSubject = ""
		cfg.MaxWaiting = c.maxWaiting
	}

	if c.ackPolicy != "" {
		cfg.AckPolicy = c.ackPolicyFromString(c.ackPolicy)
	}

	if len(c.filterSubjects) == 1 {
		cfg.FilterSubject = c.filterSubjects[0]
	} else if len(c.filterSubjects) > 1 {
		cfg.FilterSubjects = c.filterSubjects
	}

	if c.replayPolicy != "" {
		cfg.ReplayPolicy = c.replayPolicyFromString(c.replayPolicy)
	}

	if c.maxDeliver != 0 {
		cfg.MaxDeliver = c.maxDeliver
	}

	if c.bpsRateLimit != 0 {
		cfg.RateLimit = c.bpsRateLimit
	}

	if c.maxAckPending != -1 {
		cfg.MaxAckPending = c.maxAckPending
	}

	if c.idleHeartbeat != "" && c.idleHeartbeat != "-1" {
		hb, err := fisk.ParseDuration(c.idleHeartbeat)
		fisk.FatalIfError(err, "Invalid heartbeat duration")
		cfg.Heartbeat = hb
	}

	if c.description != "" {
		cfg.Description = c.description
	}

	if c.fcSet {
		cfg.FlowControl = c.fc
	}

	if cfg.DeliverSubject == "" {
		cfg.Heartbeat = 0
		cfg.FlowControl = false
	}

	if c.deliveryGroup != "_unset_" {
		cfg.DeliverGroup = c.deliveryGroup
	}

	if c.delivery == "" {
		cfg.DeliverGroup = ""
	}

	if c.deliveryGroup == "_unset_" {
		cfg.DeliverGroup = ""
	}

	if c.inactiveThreshold > 0 {
		cfg.InactiveThreshold = c.inactiveThreshold
	}

	if c.maxPullExpire > 0 {
		cfg.MaxRequestExpires = c.maxPullExpire
	}

	if c.maxPullBatch > 0 {
		cfg.MaxRequestBatch = c.maxPullBatch
	}

	if c.maxPullBytes > 0 {
		cfg.MaxRequestMaxBytes = c.maxPullBytes
	}

	if c.backoffMode != "" {
		cfg.BackOff, err = c.backoffPolicy()
		if err != nil {
			return fmt.Errorf("could not determine backoff policy: %v", err)
		}
	}

	if c.hdrsOnlySet {
		cfg.HeadersOnly = c.hdrsOnly
	}

	consumer, err := c.mgr.NewConsumerFromDefault(c.stream, cfg)
	fisk.FatalIfError(err, "Consumer creation failed")

	if cfg.Durable == "" {
		return nil
	}

	c.consumer = cfg.Durable

	c.showConsumer(consumer)

	return nil
}

func (c *consumerCmd) loadConfigFile(file string) (*api.ConsumerConfig, error) {
	f, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var cfg api.ConsumerConfig

	// there is a chance that this is a `nats c info --json` output
	// which is a ConsumerInfo, so we detect if this is one of those
	// by checking if there's a config key then extract that, else
	// we try loading it as a StreamConfig

	var nfo map[string]any
	err = json.Unmarshal(f, &nfo)
	if err != nil {
		return nil, err
	}

	_, ok := nfo["config"]
	if ok {
		var nfo api.ConsumerInfo
		err = json.Unmarshal(f, &nfo)
		if err != nil {
			return nil, err
		}
		cfg = nfo.Config
	} else {
		err = json.Unmarshal(f, &cfg)
		if err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}

func (c *consumerCmd) prepareConfig() (cfg *api.ConsumerConfig, err error) {
	cfg = c.defaultConsumer()
	cfg.Description = c.description

	if c.inputFile != "" {
		cfg, err = c.loadConfigFile(c.inputFile)
		if err != nil {
			return nil, err
		}

		if cfg.Durable != "" && c.consumer != "" && cfg.Durable != c.consumer {
			if c.consumer != "" {
				cfg.Durable = c.consumer
			} else {
				return cfg, fmt.Errorf("durable consumer name in %s does not match CLI consumer name %s", c.inputFile, c.consumer)
			}
		}

		if cfg.DeliverSubject != "" && c.delivery != "" {
			cfg.DeliverSubject = c.delivery
		}

		cfg.Metadata = iu.RemoveReservedMetadata(cfg.Metadata)

		return cfg, err
	}

	if c.consumer == "" && !c.ephemeral {
		err = iu.AskOne(&survey.Input{
			Message: "Consumer name",
			Help:    "This will be used for the name to be used when referencing this Consumer later. Settable using 'name' CLI argument",
		}, &c.consumer, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "could not request durable name")
	}

	if c.ephemeral {
		cfg.Name = c.consumer
	} else {
		cfg.Durable = c.consumer
	}

	if ok, _ := regexp.MatchString(`\.|\*|>`, cfg.Durable); ok {
		fisk.Fatalf("durable name can not contain '.', '*', '>'")
	}

	if !c.pull && c.delivery == "" {
		err = iu.AskOne(&survey.Input{
			Message: "Delivery target (empty for Pull Consumers)",
			Help:    "Consumers can be in 'push' or 'pull' mode, in 'push' mode messages are dispatched in real time to a target NATS subject, this is that subject. Leaving this blank creates a 'pull' mode Consumer. Settable using --target and --pull",
		}, &c.delivery)
		fisk.FatalIfError(err, "could not request delivery target")
	}

	cfg.DeliverSubject = c.delivery

	if c.acceptDefaults {
		if c.deliveryGroup == "_unset_" {
			c.deliveryGroup = ""
		}
		if c.startPolicy == "" {
			c.startPolicy = "all"
		}
		if c.ackPolicy == "" {
			c.ackPolicy = "none"
			if c.pull || c.delivery == "" {
				c.ackPolicy = "explicit"
			}
		}
		if c.maxDeliver == 0 {
			c.maxDeliver = -1
		}
		if c.maxAckPending == -1 {
			c.maxAckPending = 0
		}
		if c.replayPolicy == "" {
			c.replayPolicy = "instant"
		}
		if c.idleHeartbeat == "" {
			c.idleHeartbeat = "-1"
		}
		if !c.hdrsOnlySet {
			c.hdrsOnlySet = true
		}
		if cfg.DeliverSubject != "" {
			c.replayPolicy = "instant"
			c.fcSet = true
		}
	}

	if cfg.DeliverSubject != "" && c.deliveryGroup == "_unset_" {
		err = iu.AskOne(&survey.Input{
			Message: "Delivery Queue Group",
			Help:    "When set push consumers will only deliver messages to subscriptions matching this queue group",
		}, &c.deliveryGroup)
		fisk.FatalIfError(err, "could not request delivery group")
	}
	cfg.DeliverGroup = c.deliveryGroup
	if cfg.DeliverGroup == "_unset_" {
		cfg.DeliverGroup = ""
	}

	if c.startPolicy == "" {
		err = iu.AskOne(&survey.Input{
			Message: "Start policy (all, new, last, subject, 1h, msg sequence)",
			Help:    "This controls how the Consumer starts out, does it make all messages available, only the latest, latest per subject, ones after a certain time or time sequence. Settable using --deliver",
			Default: "all",
		}, &c.startPolicy, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "could not request start policy")
	}

	c.setStartPolicy(cfg, c.startPolicy)

	if c.ackPolicy == "" {
		valid := []string{"explicit", "all", "none"}
		dflt := "none"
		if c.delivery == "" {
			dflt = "explicit"
		}

		err = iu.AskOne(&survey.Select{
			Message: "Acknowledgment policy",
			Options: valid,
			Default: dflt,
			Help:    "Messages that are not acknowledged will be redelivered at a later time. 'none' means no acknowledgement is needed only 1 delivery ever, 'all' means acknowledging message 10 will also acknowledge 0-9 and 'explicit' means each has to be acknowledged specifically. Settable using --ack",
		}, &c.ackPolicy)
		fisk.FatalIfError(err, "could not ask acknowledgement policy")
	}

	if c.replayPolicy == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Replay policy",
			Options: []string{"instant", "original"},
			Default: "instant",
			Help:    "Messages can be replayed at the rate they arrived in or as fast as possible. Settable using --replay",
		}, &c.replayPolicy)
		fisk.FatalIfError(err, "could not ask replay policy")
	}

	cfg.AckPolicy = c.ackPolicyFromString(c.ackPolicy)
	if cfg.AckPolicy == api.AckNone {
		cfg.MaxDeliver = -1
	}

	if c.ackWait > 0 {
		cfg.AckWait = c.ackWait
	}

	if c.samplePct > 0 {
		if c.samplePct > 100 {
			fisk.Fatalf("sample percent is not between 0 and 100")
		}

		cfg.SampleFrequency = strconv.Itoa(c.samplePct)
	}

	if cfg.DeliverSubject != "" {
		if c.replayPolicy == "" {
			mode := ""
			err = iu.AskOne(&survey.Select{
				Message: "Replay policy",
				Options: []string{"instant", "original"},
				Default: "instant",
				Help:    "Replay policy is the time interval at which messages are delivered to interested parties. 'instant' means deliver all as soon as possible while 'original' will match the time intervals in which messages were received, useful for replaying production traffic in development. Settable using --replay",
			}, &mode)
			fisk.FatalIfError(err, "could not ask replay policy")
			c.replayPolicy = mode
		}
	}

	if c.replayPolicy != "" {
		cfg.ReplayPolicy = c.replayPolicyFromString(c.replayPolicy)
	}

	switch {
	case len(c.filterSubjects) == 0 && !c.acceptDefaults:
		sub := ""
		err = iu.AskOne(&survey.Input{
			Message: "Filter Stream by subjects (blank for all)",
			Default: "",
			Help:    "Consumers can filter messages from the stream, this is a space or comma separated list that can include wildcards. Settable using --filter",
		}, &sub)
		fisk.FatalIfError(err, "could not ask for filtering subject")
		c.filterSubjects = iu.SplitString(sub)
	}

	switch {
	case len(c.filterSubjects) == 1:
		cfg.FilterSubject = c.filterSubjects[0]
	case len(c.filterSubjects) > 1:
		cfg.FilterSubjects = c.filterSubjects
	}

	if cfg.FilterSubject == "" && len(c.filterSubjects) == 0 && cfg.DeliverPolicy == api.DeliverLastPerSubject {
		cfg.FilterSubject = ">"
	}

	if c.maxDeliver == 0 && cfg.AckPolicy != api.AckNone {
		err = iu.AskOne(&survey.Input{
			Message: "Maximum Allowed Deliveries",
			Default: "-1",
			Help:    "When this is -1 unlimited attempts to deliver an un acknowledged message is made, when this is >0 it will be maximum amount of times a message is delivered after which it is ignored. Settable using --max-deliver.",
		}, &c.maxDeliver)
		fisk.FatalIfError(err, "could not ask for maximum allowed deliveries")
	}

	if c.maxAckPending == -1 && cfg.AckPolicy != api.AckNone {
		err = iu.AskOne(&survey.Input{
			Message: "Maximum Acknowledgments Pending",
			Default: "0",
			Help:    "The maximum number of messages without acknowledgement that can be outstanding, once this limit is reached message delivery will be suspended. Settable using --max-pending.",
		}, &c.maxAckPending)
		fisk.FatalIfError(err, "could not ask for maximum outstanding acknowledgements")
	}

	if cfg.DeliverSubject != "" {
		if c.idleHeartbeat == "-1" {
			cfg.Heartbeat = 0
		} else if c.idleHeartbeat != "" {
			cfg.Heartbeat, err = fisk.ParseDuration(c.idleHeartbeat)
			fisk.FatalIfError(err, "invalid heartbeat duration")
		} else {
			idle := "0s"
			err = iu.AskOne(&survey.Input{
				Message: "Idle Heartbeat",
				Help:    "When a Push consumer is idle for the given period an empty message with a Status header of 100 will be sent to the delivery subject, settable using --heartbeat",
				Default: "0s",
			}, &idle)
			fisk.FatalIfError(err, "could not ask for idle heartbeat")
			cfg.Heartbeat, err = fisk.ParseDuration(idle)
			fisk.FatalIfError(err, "invalid heartbeat duration")
		}
	}

	if cfg.DeliverSubject != "" {
		if !c.fcSet {
			c.fc, err = askConfirmation("Enable Flow Control, ie --flow-control", false)
			fisk.FatalIfError(err, "could not ask flow control")
		}

		cfg.FlowControl = c.fc
	}

	if !c.hdrsOnlySet {
		c.hdrsOnly, err = askConfirmation("Deliver headers only without bodies", false)
		fisk.FatalIfError(err, "could not ask headers only")
	}
	cfg.HeadersOnly = c.hdrsOnly

	if cfg.AckPolicy != api.AckNone && !c.acceptDefaults && c.backoffMode == "" {
		err = c.askBackoffPolicy()
		if err != nil {
			return nil, err
		}
	}

	if c.backoffMode != "" {
		cfg.BackOff, err = c.backoffPolicy()
		if err != nil {
			return nil, fmt.Errorf("could not determine backoff policy: %v", err)
		}

		// hopefully this is just to work around a temporary bug in the server
		if c.maxDeliver == -1 && len(cfg.BackOff) > 0 {
			c.maxDeliver = len(cfg.BackOff) + 1
		}
	}

	if c.maxAckPending == -1 {
		c.maxAckPending = 0
	}

	cfg.MaxAckPending = c.maxAckPending
	cfg.InactiveThreshold = c.inactiveThreshold

	if c.maxPullBatch > 0 {
		cfg.MaxRequestBatch = c.maxPullBatch
	}

	if c.maxPullExpire > 0 {
		cfg.MaxRequestExpires = c.maxPullExpire
	}

	if c.maxPullBytes > 0 {
		cfg.MaxRequestMaxBytes = c.maxPullBytes
	}

	if cfg.DeliverSubject == "" {
		cfg.MaxWaiting = c.maxWaiting
	}

	if c.maxDeliver != 0 && cfg.AckPolicy != api.AckNone {
		cfg.MaxDeliver = c.maxDeliver
	}

	if c.bpsRateLimit > 0 && cfg.DeliverSubject == "" {
		return nil, fmt.Errorf("rate limits are only possible on Push consumers")
	}

	if cfg.DeliverSubject == "" && (c.idleHeartbeat != "" && c.idleHeartbeat != "-1") {
		return nil, fmt.Errorf("pull subscribers does not support idle heartbeats")
	}

	cfg.RateLimit = c.bpsRateLimit
	cfg.Replicas = c.replicas
	cfg.MemoryStorage = c.memory

	if c.metadataIsSet {
		cfg.Metadata = c.metadata
	}

	if c.pauseUntil != "" {
		cfg.PauseUntil, err = c.parsePauseUntil(c.pauseUntil)
		if err != nil {
			return nil, err
		}
	}

	switch {
	case len(c.pinnedGroups) > 0 && len(c.overflowGroups) > 0:
		return nil, fmt.Errorf("setting both overflow and pinned groups are not supported")
	case len(c.pinnedGroups) > 0:
		cfg.PriorityPolicy = api.PriorityPinnedClient
		cfg.PriorityGroups = c.pinnedGroups
		cfg.PinnedTTL = c.pinnedTTL
	case len(c.overflowGroups) > 0:
		cfg.PriorityPolicy = api.PriorityOverflow
		cfg.PriorityGroups = c.pinnedGroups
	}

	cfg.Metadata = iu.RemoveReservedMetadata(cfg.Metadata)

	return cfg, nil
}

func (c *consumerCmd) parsePauseUntil(until string) (time.Time, error) {
	if until == "" {
		return time.Time{}, fmt.Errorf("time not given")
	}

	var ts time.Time
	var err error

	ts, err = time.Parse(time.DateTime, until)
	if err != nil {
		dur, err := fisk.ParseDuration(until)
		if err != nil {
			return ts, fmt.Errorf("could not parse the pause time as either timestamp or duration")
		}
		ts = time.Now().Add(dur)
	}

	return ts, nil
}

func (c *consumerCmd) resumeAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(true, true)

	err := iu.RequireAPILevel(c.mgr, 1, "resuming Consumers requires NATS Server 2.11")
	if err != nil {
		return err
	}

	state, err := c.selectedConsumer.LatestState()
	if err != nil {
		return err
	}
	if !state.Paused {
		return fmt.Errorf("consumer is not paused")
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really resume Consumer %s > %s", c.stream, c.consumer), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = c.selectedConsumer.Resume()
	if err != nil {
		return err
	}

	fmt.Printf("Consumer %s > %s was resumed while previously paused until %s\n", c.stream, c.consumer, f(state.TimeStamp.Add(state.PauseRemaining)))
	return nil
}

func (c *consumerCmd) pauseAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(true, true)

	err := iu.RequireAPILevel(c.mgr, 1, "pausing Consumers requires NATS Server 2.11")
	if err != nil {
		return err
	}

	if c.pauseUntil == "" {
		dflt := time.Now().Add(time.Hour).Format(time.DateTime)
		err := iu.AskOne(&survey.Input{
			Message: "Pause until (time or duration)",
			Default: dflt,
			Help:    fmt.Sprintf("Sets the time in either a duration like 1h30m or a timestamp like '%s'", dflt),
		}, &c.pauseUntil, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	ts, err := c.parsePauseUntil(c.pauseUntil)
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really pause Consumer %s > %s until %s", c.stream, c.consumer, f(ts)), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	resp, err := c.selectedConsumer.Pause(ts)
	if err != nil {
		return err
	}

	if !resp.Paused {
		return fmt.Errorf("consumer failed to pause, perhaps a time in the past was given")
	}

	fmt.Printf("Paused %s > %s until %s (%s)\n", c.selectedConsumer.StreamName(), c.selectedConsumer.Name(), f(resp.PauseUntil), resp.PauseRemaining.Round(time.Second))
	return nil
}

func (c *consumerCmd) askBackoffPolicy() error {
	ok, err := askConfirmation("Add a Retry Backoff Policy", false)
	if err != nil {
		return err
	}

	if ok {
		err = iu.AskOne(&survey.Select{
			Message: "Backoff policy",
			Options: []string{"linear", "none"},
			Default: "none",
			Help:    "Adds a Backoff policy for use with delivery retries. Linear grows at equal intervals between min and max.",
		}, &c.backoffMode)
		if err != nil {
			return err
		}

		if c.backoffMode == "none" {
			return nil
		}

		d := ""
		err := iu.AskOne(&survey.Input{
			Message: "Minimum retry time",
			Help:    "Backoff policies range from min to max",
			Default: "1m",
		}, &d)
		if err != nil {
			return err
		}
		c.backoffMin, err = fisk.ParseDuration(d)
		if err != nil {
			return err
		}

		err = iu.AskOne(&survey.Input{
			Message: "Maximum retry time",
			Help:    "Backoff policies range from min to max",
			Default: "10m",
		}, &d)
		if err != nil {
			return err
		}
		c.backoffMax, err = fisk.ParseDuration(d)
		if err != nil {
			return err
		}

		steps, err := askOneInt("Number of steps to generate in the policy", "20", "Number of steps to create between min and max")
		if err != nil {
			return err
		}
		if steps < 1 {
			return fmt.Errorf("backoff steps must be > 0")
		}
		c.backoffSteps = uint(steps)
	}

	return nil
}

func (c *consumerCmd) validateCfg(cfg *api.ConsumerConfig) (bool, []byte, []string, error) {
	j, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return false, nil, nil, err
	}

	if os.Getenv("NOVALIDATE") != "" {
		return true, nil, nil, nil
	}

	valid, errs := cfg.Validate(new(SchemaValidator))

	return valid, j, errs, nil
}

func (c *consumerCmd) createAction(pc *fisk.ParseContext) (err error) {
	c.connectAndSetup(true, false)
	cfg, err := c.prepareConfig()
	if err != nil {
		return err
	}

	switch {
	case c.validateOnly:
		valid, j, errs, err := c.validateCfg(cfg)
		fisk.FatalIfError(err, "Could not validate configuration")

		fmt.Println(string(j))
		fmt.Println()
		if !valid {
			fisk.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		fmt.Println("Configuration is a valid Consumer")
		return nil

	case c.outFile != "":
		valid, j, errs, err := c.validateCfg(cfg)
		fisk.FatalIfError(err, "Could not validate configuration")

		if !valid {
			fisk.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		return os.WriteFile(c.outFile, j, 0600)
	}

	err = c.checkConfigLevel(cfg)
	if err != nil {
		return err
	}

	created, err := c.mgr.NewConsumerFromDefault(c.stream, *cfg)
	fisk.FatalIfError(err, "Consumer creation failed")

	c.consumer = created.Name()

	c.showConsumer(created)

	return nil
}

func (c *consumerCmd) checkConfigLevel(cfg *api.ConsumerConfig) error {
	if !cfg.PauseUntil.IsZero() {
		err := iu.RequireAPILevel(c.mgr, 1, "pausing consumers requires NATS Server 2.11")
		if err != nil {
			return err
		}
	}

	if len(cfg.PriorityGroups) > 0 || cfg.PriorityPolicy != api.PriorityNone {
		err := iu.RequireAPILevel(c.mgr, 1, "Consumer Groups requires NATS Server 2.11")
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *consumerCmd) getNextMsgDirect(stream string, consumer string) error {
	req := &api.JSApiConsumerGetNextRequest{Batch: 1, Expires: opts().Timeout}

	sub, err := c.nc.SubscribeSync(c.nc.NewRespInbox())
	fisk.FatalIfError(err, "subscribe failed")
	sub.AutoUnsubscribe(1)

	err = c.mgr.NextMsgRequest(stream, consumer, sub.Subject, req)
	fisk.FatalIfError(err, "could not request next message")

	fatalIfNotPull := func() {
		cons, err := c.mgr.LoadConsumer(stream, consumer)
		fisk.FatalIfError(err, "could not load consumer %q", consumer)

		if !cons.IsPullMode() {
			fisk.Fatalf("consumer %q is not a Pull consumer", consumer)
		}
	}

	if c.term {
		if !c.ackSetByUser {
			c.ack = false
		}

		if c.ack || c.nak {
			fisk.Fatalf("can not both Acknowledge and Terminate message")
		}

		if c.ack && c.nak {
			fisk.Fatalf("can not both Acknowledge and NaK message")
		}
	}

	msg, err := sub.NextMsg(opts().Timeout)
	if err != nil {
		fatalIfNotPull()
	}
	fisk.FatalIfError(err, "no message received")

	if msg.Header != nil && msg.Header.Get("Status") == "503" {
		fatalIfNotPull()
	}

	if !c.raw {
		info, err := jsm.ParseJSMsgMetadata(msg)
		if err != nil {
			if msg.Reply == "" {
				fmt.Printf("--- subject: %s\n", msg.Subject)
			} else {
				fmt.Printf("--- subject: %s reply: %s\n", msg.Subject, msg.Reply)
			}

		} else {
			fmt.Printf("[%s] subj: %s / tries: %d / cons seq: %d / str seq: %d / pending: %s\n", time.Now().Format("15:04:05"), msg.Subject, info.Delivered(), info.ConsumerSequence(), info.StreamSequence(), f(info.Pending()))
		}

		if len(msg.Header) > 0 {
			fmt.Println()
			fmt.Println("Headers:")
			fmt.Println()
			for h, vals := range msg.Header {
				for _, val := range vals {
					fmt.Printf("  %s: %s\n", h, val)
				}
			}

			fmt.Println()
			fmt.Println("Data:")
			fmt.Println()
		}

		fmt.Println()
		fmt.Println(string(msg.Data))
	} else {
		fmt.Println(string(msg.Data))
	}

	if c.term {
		err = msg.Term()
		fisk.FatalIfError(err, "could not Terminate message")
		c.nc.Flush()
		fmt.Println("\nTerminated message")
	}

	if c.ack || c.nak {
		var stime time.Duration
		if c.ackWait > 0 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			stime = time.Duration(r.Intn(int(c.ackWait)))
		}

		if stime > 0 {
			time.Sleep(stime)
		}

		ack := api.AckAck
		if c.nak {
			ack = api.AckNak
		}
		if opts().Trace {
			log.Printf(">>> %s: %s", msg.Reply, string(ack))
		}

		err = msg.Respond(ack)

		fisk.FatalIfError(err, "could not Acknowledge message")
		c.nc.Flush()

		if !c.raw {
			neg := ""
			if c.nak {
				neg = "Negative "
			}
			if stime > 0 {
				fmt.Printf("\n%sAcknowledged message after %s delay\n", neg, stime)
			} else {
				fmt.Printf("\n%sAcknowledged message\n", neg)
			}
			fmt.Println()
		}
	}

	return nil
}

func (c *consumerCmd) subscribeConsumer(consumer *jsm.Consumer) (err error) {
	if !c.raw {
		fmt.Printf("Subscribing to topic %s auto acknowledgment: %v\n\n", consumer.DeliverySubject(), c.ack)
		fmt.Println("Consumer Info:")
		fmt.Printf("  Ack Policy: %s\n", consumer.AckPolicy().String())
		if consumer.AckPolicy() != api.AckNone {
			fmt.Printf("    Ack Wait: %v\n", consumer.AckWait())
		}
		fmt.Println()
	}

	handler := func(m *nats.Msg) {
		if len(m.Data) == 0 && m.Header.Get("Status") == "100" {
			stalled := m.Header.Get("Nats-Consumer-Stalled")
			if stalled != "" {
				c.nc.Publish(stalled, nil)
			} else {
				m.Respond(nil)
			}

			return
		}

		var msginfo *jsm.MsgInfo
		var err error

		if len(m.Reply) > 0 {
			msginfo, err = jsm.ParseJSMsgMetadata(m)
		}

		fisk.FatalIfError(err, "could not parse JetStream metadata: '%s'", m.Reply)

		if !c.raw {
			now := time.Now().Format("15:04:05")

			if msginfo != nil {
				fmt.Printf("[%s] subj: %s / tries: %d / cons seq: %d / str seq: %d / pending: %s\n", now, m.Subject, msginfo.Delivered(), msginfo.ConsumerSequence(), msginfo.StreamSequence(), f(msginfo.Pending()))
			} else {
				fmt.Printf("[%s] %s reply: %s\n", now, m.Subject, m.Reply)
			}

			if len(m.Header) > 0 {
				if len(m.Data) == 0 && m.Reply != "" && m.Header.Get("Status") == "100" {
					m.Respond(nil)
					return
				}

				fmt.Println()
				fmt.Println("Headers:")
				fmt.Println()

				for h, vals := range m.Header {
					for _, val := range vals {
						fmt.Printf("   %s: %s\n", h, val)
					}
				}

				fmt.Println()
				fmt.Println("Data:")
			}

			fmt.Printf("%s\n", string(m.Data))
			if !strings.HasSuffix(string(m.Data), "\n") {
				fmt.Println()
			}
		} else {
			fmt.Println(string(m.Data))
		}

		if c.ack {
			err = m.Respond(nil)
			if err != nil {
				fmt.Printf("Acknowledging message via subject %s failed: %s\n", m.Reply, err)
			}
		}
	}

	if consumer.DeliverGroup() == "" {
		_, err = c.nc.Subscribe(consumer.DeliverySubject(), handler)
	} else {
		_, err = c.nc.QueueSubscribe(consumer.DeliverySubject(), consumer.DeliverGroup(), handler)
	}

	fisk.FatalIfError(err, "could not subscribe")

	<-ctx.Done()

	return nil
}

func (c *consumerCmd) subAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(true, true, nats.UseOldRequestStyle())

	consumer, err := c.mgr.LoadConsumer(c.stream, c.consumer)
	fisk.FatalIfError(err, "could not load Consumer")

	if consumer.AckPolicy() == api.AckNone {
		c.ack = false
	}

	switch {
	case consumer.IsPullMode():
		return c.getNextMsgDirect(consumer.StreamName(), consumer.Name())
	case consumer.IsPushMode():
		return c.subscribeConsumer(consumer)
	default:
		return fmt.Errorf("consumer %s > %s is in an unknown state", c.stream, c.consumer)
	}
}

func (c *consumerCmd) nextAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(false, false, nats.UseOldRequestStyle())

	var err error

	for i := 0; i < c.pullCount; i++ {
		err = c.getNextMsgDirect(c.stream, c.consumer)
		if err != nil {
			break
		}
	}
	return err
}

func (c *consumerCmd) connectAndSetup(askStream bool, askConsumer bool, opts ...nats.Option) {
	var err error

	if c.nc == nil || c.mgr == nil {
		c.nc, c.mgr, err = prepareHelper("", append(natsOpts(), opts...)...)
		fisk.FatalIfError(err, "setup failed")
	}

	if c.stream != "" && c.consumer != "" {
		c.selectedConsumer, err = c.mgr.LoadConsumer(c.stream, c.consumer)
		if err == nil {
			return
		}
	}

	if askStream {
		c.stream, _, err = selectStream(c.mgr, c.stream, c.force, c.showAll)
		fisk.FatalIfError(err, "could not select Stream")

		if askConsumer {
			c.consumer, c.selectedConsumer, err = selectConsumer(c.mgr, c.stream, c.consumer, c.force)
			fisk.FatalIfError(err, "could not select Consumer")
		}
	}
}

func (c *consumerCmd) reportAction(_ *fisk.ParseContext) error {
	c.connectAndSetup(true, false)

	s, err := c.mgr.LoadStream(c.stream)
	if err != nil {
		return err
	}

	ss, err := s.LatestState()
	if err != nil {
		return err
	}

	leaders := make(map[string]*raftLeader)

	table := iu.NewTableWriter(opts(), fmt.Sprintf("Consumer report for %s with %s consumers", c.stream, f(ss.Consumers)))
	table.AddHeaders("Consumer", "Mode", "Ack Policy", "Ack Wait", "Ack Pending", "Redelivered", "Unprocessed", "Ack Floor", "Cluster")
	missing, err := s.EachConsumer(func(cons *jsm.Consumer) {
		cs, err := cons.LatestState()
		if err != nil {
			log.Printf("Could not obtain consumer state for %s: %s", cons.Name(), err)
			return
		}

		mode := "Push"
		if cons.IsPullMode() {
			mode = "Pull"
		}

		if cs.Cluster != nil {
			if cs.Cluster.Leader != "" {
				_, ok := leaders[cs.Cluster.Leader]
				if !ok {
					leaders[cs.Cluster.Leader] = &raftLeader{name: cs.Cluster.Leader, cluster: cs.Cluster.Name}
				}
				leaders[cs.Cluster.Leader].groups++
			}
		}

		if c.raw {
			table.AddRow(cons.Name(), mode, cons.AckPolicy().String(), cons.AckWait(), cs.NumAckPending, cs.NumRedelivered, cs.NumPending, cs.AckFloor.Stream, renderCluster(cs.Cluster))
		} else {
			unprocessed := "0"
			if cs.NumPending > 0 {
				upct := math.Floor(float64(cs.NumPending) / float64(ss.Msgs) * 100)
				if upct > 100 {
					upct = 100
				}
				unprocessed = fmt.Sprintf("%s / %0.0f%%", f(cs.NumPending), upct)
			}

			table.AddRow(cons.Name(), mode, cons.AckPolicy().String(), f(cons.AckWait()), f(cs.NumAckPending), f(cs.NumRedelivered), unprocessed, f(cs.AckFloor.Stream), renderCluster(cs.Cluster))
		}
	})
	if err != nil {
		return err
	}

	fmt.Println(table.Render())

	if c.reportLeaderDistrib && len(leaders) > 0 {
		renderRaftLeaders(leaders, "Consumers")
	}

	if len(missing) > 0 {
		c.renderMissing(os.Stdout, missing)
	}

	return nil
}

func (c *consumerCmd) renderMissing(out io.Writer, missing []string) {
	toany := func(items []string) (res []any) {
		for _, i := range items {
			res = append(res, any(i))
		}
		return res
	}

	if len(missing) > 0 {
		fmt.Fprintln(out)
		sort.Strings(missing)
		table := iu.NewTableWriter(opts(), "Inaccessible Consumers")
		iu.SliceGroups(missing, 4, func(names []string) {
			table.AddRow(toany(names)...)
		})
		fmt.Fprint(out, table.Render())
	}
}
