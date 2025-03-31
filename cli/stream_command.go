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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"

	"github.com/nats-io/natscli/internal/asciigraph"
	iu "github.com/nats-io/natscli/internal/util"
	terminal "golang.org/x/term"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/emicklei/dot"
	"github.com/google/go-cmp/cmp"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/balancer"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/columns"
	"gopkg.in/yaml.v3"
)

type streamCmd struct {
	stream           string
	force            bool
	json             bool
	msgID            int64
	retentionPolicyS string
	inputFile        string
	outFile          string
	filterSubject    string
	showAll          bool
	acceptDefaults   bool

	destination            string
	subjects               []string
	ack                    bool
	storage                string
	maxMsgLimit            int64
	maxMsgPerSubjectLimit  int64
	maxBytesLimitString    string
	maxBytesLimit          int64
	maxAgeLimit            string
	maxMsgSizeString       string
	maxMsgSize             int64
	maxConsumers           int
	reportSortConsumers    bool
	reportSortMsgs         bool
	reportSortName         bool
	reportSortReverse      bool
	reportSortStorage      bool
	reportSort             string
	reportRaw              bool
	reportLimitCluster     string
	reportLeaderDistrib    bool
	discardPolicy          string
	validateOnly           bool
	backupDirectory        string
	showProgress           bool
	healthCheck            bool
	snapShotConsumers      bool
	dupeWindow             string
	replicas               int64
	placementCluster       string
	placementTags          []string
	placementClusterSet    bool
	placementTagsSet       bool
	peerName               string
	sources                []string
	mirror                 string
	interactive            bool
	purgeKeep              uint64
	purgeSubject           string
	purgeSequence          uint64
	description            string
	subjectTransformSource string
	subjectTransformDest   string
	noSubjectTransform     bool
	repubSource            string
	repubDest              string
	repubHeadersOnly       bool
	noRepub                bool
	allowRollup            bool
	allowRollupSet         bool
	denyDelete             bool
	denyDeleteSet          bool
	denyPurge              bool
	denyPurgeSet           bool
	allowDirect            bool
	allowDirectSet         bool
	allowMirrorDirect      bool
	allowMirrorDirectSet   bool
	discardPerSubj         bool
	discardPerSubjSet      bool
	showStateOnly          bool
	metadata               map[string]string
	metadataIsSet          bool
	compression            string
	compressionSet         bool
	firstSeq               uint64
	limitInactiveThreshold time.Duration
	limitMaxAckPending     int

	fServer      string
	fCluster     string
	fEmpty       bool
	fIdle        time.Duration
	fCreated     time.Duration
	fConsumers   int
	fInvert      bool
	fReplicas    uint
	fSourced     bool
	fSourcedSet  bool
	fMirrored    bool
	fMirroredSet bool
	fExpression  string
	fLeader      string

	listNames    bool
	vwStartId    int
	vwStartDelta time.Duration
	vwPageSize   int
	vwRaw        bool
	vwTranslate  string
	vwSubject    string

	dryRun                    bool
	selectedStream            *jsm.Stream
	nc                        *nats.Conn
	mgr                       *jsm.Manager
	chunkSize                 string
	placementPreferred        string
	allowMsgTTlSet            bool
	allowMsgTTL               bool
	subjectDeleteMarkerTTLSet bool
	subjectDeleteMarkerTTL    time.Duration
}

type streamStat struct {
	Name      string
	Consumers int
	Msgs      int64
	Bytes     uint64
	Storage   string
	Template  string
	Cluster   *api.ClusterInfo
	LostBytes uint64
	LostMsgs  int
	Deleted   int
	Mirror    *api.StreamSourceInfo
	Sources   []*api.StreamSourceInfo
	Placement *api.Placement
}

func configureStreamCommand(app commandHost) {
	c := &streamCmd{msgID: -1, metadata: map[string]string{}}

	addCreateFlags := func(f *fisk.CmdClause, edit bool) {
		f.Flag("subjects", "Subjects that are consumed by the Stream").Default().StringsVar(&c.subjects)
		f.Flag("description", "Sets a contextual description for the stream").StringVar(&c.description)
		if !edit {
			f.Flag("storage", "Storage backend to use (file, memory)").EnumVar(&c.storage, "file", "f", "memory", "m")
		}
		f.Flag("compression", "Compression algorithm to use (file storage only)").IsSetByUser(&c.compressionSet).EnumVar(&c.compression, "none", "s2")
		f.Flag("replicas", "When clustered, how many replicas of the data to create").Int64Var(&c.replicas)
		f.Flag("tag", "Place the stream on servers that has specific tags (pass multiple times)").IsSetByUser(&c.placementTagsSet).StringsVar(&c.placementTags)
		f.Flag("tags", "Backward compatibility only, use --tag").Hidden().IsSetByUser(&c.placementTagsSet).StringsVar(&c.placementTags)
		f.Flag("cluster", "Place the stream on a specific cluster").IsSetByUser(&c.placementClusterSet).StringVar(&c.placementCluster)
		f.Flag("ack", "Acknowledge publishes").Default("true").BoolVar(&c.ack)
		if !edit {
			f.Flag("retention", "Defines a retention policy (limits, interest, work)").EnumVar(&c.retentionPolicyS, "limits", "interest", "workq", "work")
		}
		f.Flag("discard", "Defines the discard policy (new, old)").EnumVar(&c.discardPolicy, "new", "old")
		f.Flag("discard-per-subject", "Sets the 'new' discard policy and applies it to every subject in the stream").IsSetByUser(&c.discardPerSubjSet).BoolVar(&c.discardPerSubj)
		if !edit {
			f.Flag("first-sequence", "Sets the starting sequence").Uint64Var(&c.firstSeq)
		}
		f.Flag("max-age", "Maximum age of messages to keep").Default("").StringVar(&c.maxAgeLimit)
		f.Flag("max-bytes", "Maximum bytes to keep").PlaceHolder("BYTES").StringVar(&c.maxBytesLimitString)
		f.Flag("max-consumers", "Maximum number of consumers to allow").Default("-1").IntVar(&c.maxConsumers)
		f.Flag("max-msg-size", "Maximum size any 1 message may be").PlaceHolder("BYTES").StringVar(&c.maxMsgSizeString)
		f.Flag("max-msgs", "Maximum amount of messages to keep").Default("0").Int64Var(&c.maxMsgLimit)
		f.Flag("max-msgs-per-subject", "Maximum amount of messages to keep per subject").Default("0").Int64Var(&c.maxMsgPerSubjectLimit)
		f.Flag("dupe-window", "Duration of the duplicate message tracking window").Default("").StringVar(&c.dupeWindow)
		f.Flag("mirror", "Completely mirror another stream").StringVar(&c.mirror)
		f.Flag("source", "Source data from other Streams, merging into this one").PlaceHolder("STREAM").StringsVar(&c.sources)
		f.Flag("allow-rollup", "Allows roll-ups to be done by publishing messages with special headers").IsSetByUser(&c.allowRollupSet).BoolVar(&c.allowRollup)
		f.Flag("deny-delete", "Deny messages from being deleted via the API").IsSetByUser(&c.denyDeleteSet).BoolVar(&c.denyDelete)
		f.Flag("deny-purge", "Deny entire stream or subject purges via the API").IsSetByUser(&c.denyPurgeSet).BoolVar(&c.denyPurge)
		f.Flag("allow-direct", "Allows fast, direct, access to stream data via the direct get API").IsSetByUser(&c.allowDirectSet).Default("true").BoolVar(&c.allowDirect)
		f.Flag("allow-mirror-direct", "Allows fast, direct, access to stream data via the direct get API on mirrors").IsSetByUser(&c.allowMirrorDirectSet).BoolVar(&c.allowMirrorDirect)
		if !edit {
			f.Flag("allow-msg-ttl", "Allows per-message TTL handling").IsSetByUser(&c.allowMsgTTlSet).UnNegatableBoolVar(&c.allowMsgTTL)
		}
		f.Flag("subject-del-markers-ttl", "How long delete markers should persist in the Stream").PlaceHolder("DURATION").IsSetByUser(&c.subjectDeleteMarkerTTLSet).DurationVar(&c.subjectDeleteMarkerTTL)
		f.Flag("transform-source", "Stream subject transform source").PlaceHolder("SOURCE").StringVar(&c.subjectTransformSource)
		f.Flag("transform-destination", "Stream subject transform destination").PlaceHolder("DEST").StringVar(&c.subjectTransformDest)
		f.Flag("metadata", "Adds metadata to the stream").PlaceHolder("META").IsSetByUser(&c.metadataIsSet).StringMapVar(&c.metadata)
		f.Flag("republish-source", "Republish messages to --republish-destination").PlaceHolder("SOURCE").StringVar(&c.repubSource)
		f.Flag("republish-destination", "Republish destination for messages in --republish-source").PlaceHolder("DEST").StringVar(&c.repubDest)
		f.Flag("republish-headers", "Republish only message headers, no bodies").UnNegatableBoolVar(&c.repubHeadersOnly)
		if !edit {
			f.Flag("limit-consumer-inactive", "The maximum Consumer inactive threshold the Stream allows").PlaceHolder("THRESHOLD").DurationVar(&c.limitInactiveThreshold)
			f.Flag("limit-consumer-max-pending", "The maximum Consumer Ack Pending the stream Allows").PlaceHolder("PENDING").IntVar(&c.limitMaxAckPending)
		}

		if edit {
			f.Flag("no-republish", "Removes current republish configuration").UnNegatableBoolVar(&c.noRepub)
			f.Flag("no-transform", "Removes current subject transform configuration").UnNegatableBoolVar(&c.noSubjectTransform)
		}

		f.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

		f.PreAction(c.parseLimitStrings)
	}

	str := app.Command("stream", "JetStream Stream management").Alias("str").Alias("st").Alias("ms").Alias("s")
	str.Flag("all", "When listing or selecting streams show all streams including system ones").Short('a').UnNegatableBoolVar(&c.showAll)
	addCheat("stream", str)

	strAdd := str.Command("add", "Create a new Stream").Alias("create").Alias("new").Action(c.addAction)
	strAdd.Arg("stream", "Stream name").StringVar(&c.stream)
	strAdd.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	strAdd.Flag("validate", "Only validates the configuration against the official Schema").UnNegatableBoolVar(&c.validateOnly)
	strAdd.Flag("output", "Save configuration instead of creating").PlaceHolder("FILE").StringVar(&c.outFile)
	addCreateFlags(strAdd, false)
	strAdd.Flag("defaults", "Accept default values for all prompts").UnNegatableBoolVar(&c.acceptDefaults)

	strLs := str.Command("ls", "List all known Streams").Alias("list").Alias("l").Action(c.lsAction)
	strLs.Flag("subject", "Limit the list to streams with matching subjects").StringVar(&c.filterSubject)
	strLs.Flag("names", "Show just the stream names").Short('n').UnNegatableBoolVar(&c.listNames)
	strLs.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	strReport := str.Command("report", "Reports on Stream statistics").Action(c.reportAction)
	strReport.Flag("subject", "Limit the report to streams with matching subjects").StringVar(&c.filterSubject)
	strReport.Flag("cluster", "Limit report to streams within a specific cluster").StringVar(&c.reportLimitCluster)
	strReport.Flag("consumers", "Sort by number of Consumers").Short('o').UnNegatableBoolVar(&c.reportSortConsumers)
	strReport.Flag("messages", "Sort by number of Messages").Short('m').UnNegatableBoolVar(&c.reportSortMsgs)
	strReport.Flag("name", "Sort by Stream name").Short('n').UnNegatableBoolVar(&c.reportSortName)
	strReport.Flag("storage", "Sort by Storage type").Short('t').UnNegatableBoolVar(&c.reportSortStorage)
	strReport.Flag("raw", "Show un-formatted numbers").Short('r').UnNegatableBoolVar(&c.reportRaw)
	strReport.Flag("dot", "Produce a GraphViz graph of replication topology").StringVar(&c.outFile)
	strReport.Flag("leaders", "Show details about cluster leaders").Short('l').UnNegatableBoolVar(&c.reportLeaderDistrib)

	findHelp := `Expression format:

Using the --expression flag arbitrary complex matching can be done across any state field(s) from the stream information.

Use this when trying to match on fields we don't specifically support or to perform complex boolean matches.

We use the expr language to perform matching, see https://expr.medv.io/docs/Language-Definition for detail about the expression language.  Expressions you enter must return a boolean value.

The following items are available to query, all using the same values seen in JSON from stream info:

  * config - Stream configuration
  * state - Stream state
  * info - Stream information aka state.state

Additionally there is Info (with a capital I) that is the golang structure and with keys matching the struct keys in the server.

Finding streams with more than 10 messages:

   nats s find --expression 'state.messages < 10'
   nats s find --expression 'Info.State.Msgs < 10'

Finding streams in multiple clusters:

   nats s find --expression 'info.cluster.name in ["lon", "sfo"]'

Finding streams with certain subjects configured:

   nats s find --expression '"js.in.orders_1" in config.subjects'
`
	strFind := str.Command("find", "Finds streams matching certain criteria").Alias("query").Action(c.findAction)
	strFind.HelpLong(findHelp)
	strFind.Flag("server-name", "Display streams present on a regular expression matched server").StringVar(&c.fServer)
	strFind.Flag("cluster", "Display streams present on a regular expression matched cluster").StringVar(&c.fCluster)
	strFind.Flag("empty", "Display streams with no messages").UnNegatableBoolVar(&c.fEmpty)
	strFind.Flag("idle", "Display streams with no new messages or consumer deliveries for a period").PlaceHolder("DURATION").DurationVar(&c.fIdle)
	strFind.Flag("created", "Display streams created longer ago than duration").PlaceHolder("DURATION").DurationVar(&c.fCreated)
	strFind.Flag("consumers", "Display streams with fewer consumers than threshold").PlaceHolder("THRESHOLD").Default("-1").IntVar(&c.fConsumers)
	strFind.Flag("subject", "Filters Streams by those with interest matching a subject or wildcard").StringVar(&c.filterSubject)
	strFind.Flag("replicas", "Display streams with fewer or equal replicas than the value").PlaceHolder("REPLICAS").UintVar(&c.fReplicas)
	strFind.Flag("sourced", "Display that sources data from other streams").IsSetByUser(&c.fSourcedSet).UnNegatableBoolVar(&c.fSourced)
	strFind.Flag("mirrored", "Display that mirrors data from other streams").IsSetByUser(&c.fMirroredSet).UnNegatableBoolVar(&c.fMirrored)
	strFind.Flag("leader", "Display only clustered streams with a specific leader").PlaceHolder("SERVER").StringVar(&c.fLeader)
	strFind.Flag("names", "Show just the stream names").Short('n').UnNegatableBoolVar(&c.listNames)
	strFind.Flag("invert", "Invert the check - before becomes after, with becomes without").BoolVar(&c.fInvert)
	strFind.Flag("expression", "Match streams using an expression language").StringVar(&c.fExpression)

	strInfo := str.Command("info", "Stream information").Alias("nfo").Alias("i").Action(c.infoAction)
	strInfo.Arg("stream", "Stream to retrieve information for").StringVar(&c.stream)
	strInfo.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	strInfo.Flag("state", "Shows only the stream state").UnNegatableBoolVar(&c.showStateOnly)
	strInfo.Flag("no-select", "Do not select streams from a list").Default("false").UnNegatableBoolVar(&c.force)

	strState := str.Command("state", "Stream state").Action(c.stateAction)
	strState.Arg("stream", "Stream to retrieve state information for").StringVar(&c.stream)
	strState.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	strState.Flag("no-select", "Do not select streams from a list").Default("false").UnNegatableBoolVar(&c.force)

	strSubs := str.Command("subjects", "Query subjects held in a stream").Alias("subj").Action(c.subjectsAction)
	strSubs.Arg("stream", "Stream name").StringVar(&c.stream)
	strSubs.Arg("filter", "Limit the subjects to those matching a filter").Default(">").StringVar(&c.filterSubject)
	strSubs.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	strSubs.Flag("sort", "Adjusts the sorting order (name, messages)").Default("messages").EnumVar(&c.reportSort, "name", "subjects", "messages", "count")
	strSubs.Flag("reverse", "Reverse sort servers").Short('R').UnNegatableBoolVar(&c.reportSortReverse)
	strSubs.Flag("names", "SList only subject names").BoolVar(&c.listNames)

	strEdit := str.Command("edit", "Edits an existing stream").Alias("update").Action(c.editAction)
	strEdit.Arg("stream", "Stream to retrieve edit").StringVar(&c.stream)
	strEdit.Flag("config", "JSON file to read configuration from").ExistingFileVar(&c.inputFile)
	strEdit.Flag("force", "Force edit without prompting").Short('f').UnNegatableBoolVar(&c.force)
	strEdit.Flag("interactive", "Edit the configuring using your editor").Short('i').BoolVar(&c.interactive)
	strEdit.Flag("dry-run", "Only shows differences, do not edit the stream").UnNegatableBoolVar(&c.dryRun)
	addCreateFlags(strEdit, true)

	strRm := str.Command("rm", "Removes a Stream").Alias("delete").Alias("del").Action(c.rmAction)
	strRm.Arg("stream", "Stream name").StringVar(&c.stream)
	strRm.Flag("force", "Force removal without prompting").Short('f').UnNegatableBoolVar(&c.force)

	strPurge := str.Command("purge", "Purge a Stream without deleting it").Action(c.purgeAction)
	strPurge.Arg("stream", "Stream name").StringVar(&c.stream)
	strPurge.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	strPurge.Flag("force", "Force removal without prompting").Short('f').UnNegatableBoolVar(&c.force)
	strPurge.Flag("subject", "Limits the purge to a specific subject").PlaceHolder("SUBJECT").StringVar(&c.purgeSubject)
	strPurge.Flag("seq", "Purge up to but not including a specific message sequence").PlaceHolder("SEQUENCE").Uint64Var(&c.purgeSequence)
	strPurge.Flag("keep", "Keeps a certain number of messages after the purge").PlaceHolder("MESSAGES").Uint64Var(&c.purgeKeep)

	strCopy := str.Command("copy", "Creates a new Stream based on the configuration of another, does not copy data").Alias("cp").Action(c.cpAction)
	strCopy.Arg("source", "Source Stream to copy").Required().StringVar(&c.stream)
	strCopy.Arg("destination", "New Stream to create").Required().StringVar(&c.destination)
	addCreateFlags(strCopy, false)

	strRmMsg := str.Command("rmm", "Securely removes an individual message from a Stream").Action(c.rmMsgAction)
	strRmMsg.Arg("stream", "Stream name").StringVar(&c.stream)
	strRmMsg.Arg("id", "Message Sequence to remove").Int64Var(&c.msgID)
	strRmMsg.Flag("force", "Force removal without prompting").Short('f').UnNegatableBoolVar(&c.force)

	strView := str.Command("view", "View messages in a stream").Action(c.viewAction)
	strView.Arg("stream", "Stream name").StringVar(&c.stream)
	strView.Arg("size", "Page size").Default("10").IntVar(&c.vwPageSize)
	strView.Flag("id", "Start at a specific message Sequence").IntVar(&c.vwStartId)
	strView.Flag("since", "Delivers messages received since a duration like 1d3h5m2s").DurationVar(&c.vwStartDelta)
	strView.Flag("raw", "Show the raw data received").UnNegatableBoolVar(&c.vwRaw)
	strView.Flag("translate", "Translate the message data by running it through the given command before output").StringVar(&c.vwTranslate)
	strView.Flag("subject", "Filter the stream using a subject").StringVar(&c.vwSubject)

	strGet := str.Command("get", "Retrieves a specific message from a Stream").Action(c.getAction)
	strGet.Arg("stream", "Stream name").StringVar(&c.stream)
	strGet.Arg("id", "Message Sequence to retrieve").Int64Var(&c.msgID)
	strGet.Flag("last-for", "Retrieves the message for a specific subject").Short('S').PlaceHolder("SUBJECT").StringVar(&c.filterSubject)
	strGet.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)
	strGet.Flag("translate", "Translate the message data by running it through the given command before output").StringVar(&c.vwTranslate)

	strBackup := str.Command("backup", "Creates a backup of a Stream over the NATS network").Alias("snapshot").Action(c.backupAction)
	strBackup.Arg("stream", "Stream to backup").Required().StringVar(&c.stream)
	strBackup.Arg("target", "Directory to create the backup in").Required().StringVar(&c.backupDirectory)
	strBackup.Flag("progress", "Enables or disables progress reporting using a progress bar").Default("true").BoolVar(&c.showProgress)
	strBackup.Flag("check", "Checks the Stream for health prior to backup").UnNegatableBoolVar(&c.healthCheck)
	strBackup.Flag("consumers", "Enable or disable consumer backups").Default("true").BoolVar(&c.snapShotConsumers)
	strBackup.Flag("chunk-size", "Sets a specific chunk size that the server will send").PlaceHolder("BYTES").Default("128KB").StringVar(&c.chunkSize)

	strRestore := str.Command("restore", "Restore a Stream over the NATS network").Action(c.restoreAction)
	strRestore.Arg("file", "The directory holding the backup to restore").Required().ExistingDirVar(&c.backupDirectory)
	strRestore.Flag("progress", "Enables or disables progress reporting using a progress bar").Default("true").BoolVar(&c.showProgress)
	strRestore.Flag("config", "Load a different configuration when restoring the stream").ExistingFileVar(&c.inputFile)
	strRestore.Flag("cluster", "Place the stream in a specific cluster").StringVar(&c.placementCluster)
	strRestore.Flag("tag", "Place the stream on servers that has specific tags (pass multiple times)").StringsVar(&c.placementTags)
	strRestore.Flag("replicas", "Override how many replicas of the data to create").Int64Var(&c.replicas)

	strSeal := str.Command("seal", "Seals a stream preventing further updates").Action(c.sealAction)
	strSeal.Arg("stream", "The name of the Stream to seal").Required().StringVar(&c.stream)
	strSeal.Flag("force", "Force sealing without prompting").Short('f').UnNegatableBoolVar(&c.force)

	gapDetect := str.Command("gaps", "Detect gaps in the Stream content that would be reported as deleted messages").Action(c.detectGaps)
	gapDetect.Arg("stream", "Stream to act on").StringVar(&c.stream)
	gapDetect.Flag("force", "Act without prompting").Short('f').UnNegatableBoolVar(&c.force)
	gapDetect.Flag("progress", "Enable progress bar").Default("true").BoolVar(&c.showProgress)
	gapDetect.Flag("json", "Show detected gaps in JSON format").UnNegatableBoolVar(&c.json)

	graph := str.Command("graph", "View a graph of Stream activity").Action(c.graphAction)
	graph.Arg("stream", "The name of the Stream to graph").StringVar(&c.stream)

	strCluster := str.Command("cluster", "Manages a clustered Stream").Alias("c")
	strClusterDown := strCluster.Command("step-down", "Force a new leader election by standing down the current leader").Alias("stepdown").Alias("sd").Alias("elect").Alias("down").Alias("d").Action(c.leaderStandDown)
	strClusterDown.Arg("stream", "Stream to act on").StringVar(&c.stream)
	strClusterDown.Flag("preferred", "Prefer placing the leader on a specific host").StringVar(&c.placementPreferred)
	strClusterDown.Flag("force", "Force leader step down ignoring current leader").Short('f').UnNegatableBoolVar(&c.force)

	strClusterBalance := strCluster.Command("balance", "Balance stream leaders").Action(c.balanceAction)
	strClusterBalance.Flag("server-name", "Balance streams present on a regular expression matched server").StringVar(&c.fServer)
	strClusterBalance.Flag("cluster", "Balance streams present on a regular expression matched cluster").StringVar(&c.fCluster)
	strClusterBalance.Flag("empty", "Balance streams with no messages").UnNegatableBoolVar(&c.fEmpty)
	strClusterBalance.Flag("idle", "Balance streams with no new messages or consumer deliveries for a period").PlaceHolder("DURATION").DurationVar(&c.fIdle)
	strClusterBalance.Flag("created", "Balance streams created longer ago than duration").PlaceHolder("DURATION").DurationVar(&c.fCreated)
	strClusterBalance.Flag("consumers", "Balance streams with fewer consumers than threshold").PlaceHolder("THRESHOLD").Default("-1").IntVar(&c.fConsumers)
	strClusterBalance.Flag("subject", "Filters Streams by those with interest matching a subject or wildcard and balances them").StringVar(&c.filterSubject)
	strClusterBalance.Flag("replicas", "Balance streams with fewer or equal replicas than the value").PlaceHolder("REPLICAS").UintVar(&c.fReplicas)
	strClusterBalance.Flag("sourced", "Balance that sources data from other streams").IsSetByUser(&c.fSourcedSet).UnNegatableBoolVar(&c.fSourced)
	strClusterBalance.Flag("mirrored", "Balance that mirrors data from other streams").IsSetByUser(&c.fMirroredSet).UnNegatableBoolVar(&c.fMirrored)
	strClusterBalance.Flag("leader", "Balance only clustered streams with a specific leader").PlaceHolder("SERVER").StringVar(&c.fLeader)
	strClusterBalance.Flag("invert", "Invert the check - before becomes after, with becomes without").BoolVar(&c.fInvert)
	strClusterBalance.Flag("expression", "Balance matching streams using an expression language").StringVar(&c.fExpression)

	strClusterRemovePeer := strCluster.Command("peer-remove", "Removes a peer from the Stream cluster").Alias("pr").Action(c.removePeer)
	strClusterRemovePeer.Arg("stream", "The stream to act on").StringVar(&c.stream)
	strClusterRemovePeer.Arg("peer", "The name of the peer to remove").StringVar(&c.peerName)
	strClusterRemovePeer.Flag("force", "Force sealing without prompting").Short('f').UnNegatableBoolVar(&c.force)
}

func init() {
	registerCommand("stream", 16, configureStreamCommand)
}

func (c *streamCmd) kvAbstractionWarn(stream string, prompt string) error {
	fmt.Println("WARNING: Operating on the underlying stream of a Key-Value bucket is dangerous.")
	fmt.Println()
	fmt.Println("Key-Value stores are an abstraction above JetStream Streams and as such require particular")
	fmt.Println("configuration to be set. Interacting with KV buckets outside of the 'nats kv' subcommand can lead")
	fmt.Println("unexpected outcomes, data loss and, technically, will mean your KV bucket is no longer a KV bucket.")
	fmt.Println()
	fmt.Println("Continuing this operation is an unsupported action.")
	fmt.Println()

	ans, err := askConfirmation(prompt, false)
	if err != nil {
		return err
	}

	if !ans {
		return fmt.Errorf("aborting Key-Value store operation")
	}

	return nil
}

func (c *streamCmd) graphAction(_ *fisk.ParseContext) error {
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

	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	nfo, err := stream.State()
	if err != nil {
		return err
	}

	messageRates := make([]float64, width)
	messagesStored := make([]float64, width)
	limitedRates := make([]float64, width)
	lastLastSeq := nfo.LastSeq
	lastFirstSeq := nfo.FirstSeq
	lastStateTs := time.Now()

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
				width -= 11
			}
			if height > 10 {
				height -= 6
			}

			if width < 20 || height < 20 {
				return fmt.Errorf("please increase terminal dimensions")
			}

			nfo, err := stream.State()
			if err != nil {
				continue
			}

			messagesStored = append(messagesStored, float64(nfo.Msgs))
			messageRates = append(messageRates, calculateRate(float64(nfo.LastSeq), float64(lastLastSeq), time.Since(lastStateTs)))
			limitedRates = append(limitedRates, calculateRate(float64(nfo.FirstSeq), float64(lastFirstSeq), time.Since(lastStateTs)))

			lastStateTs = time.Now()
			lastLastSeq = nfo.LastSeq
			lastFirstSeq = nfo.FirstSeq

			messageRates = resizeData(messageRates, width)
			messagesStored = resizeData(messagesStored, width)
			limitedRates = resizeData(limitedRates, width)

			messagesPlot := asciigraph.Plot(messagesStored,
				asciigraph.Caption("Messages Stored"),
				asciigraph.Width(width),
				asciigraph.Height(height/3-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(fFloat2Int),
			)

			limitedRatePlot := asciigraph.Plot(limitedRates,
				asciigraph.Caption("Messages Removed / second"),
				asciigraph.Width(width),
				asciigraph.Height(height/3-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(f),
			)

			msgRatePlot := asciigraph.Plot(messageRates,
				asciigraph.Caption("Messages Stored / second"),
				asciigraph.Width(width),
				asciigraph.Height(height/3-2),
				asciigraph.LowerBound(0),
				asciigraph.Precision(0),
				asciigraph.ValueFormatter(f),
			)

			iu.ClearScreen()

			fmt.Printf("Stream Statistics for %s\n", c.stream)
			fmt.Println()
			fmt.Println(messagesPlot)
			fmt.Println()
			fmt.Println(limitedRatePlot)
			fmt.Println()
			fmt.Println(msgRatePlot)

		case <-ctx.Done():
			iu.ClearScreen()
			return nil
		}
	}
}

func (c *streamCmd) detectGaps(_ *fisk.ParseContext) error {
	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	if info.State.NumDeleted == 0 {
		if c.json {
			fmt.Println("{}")
		}

		fmt.Printf("No deleted messages in %s\n", c.stream)
		return nil
	}

	if !c.force {
		fmt.Println("WARNING: Detecting gaps in a stream consumes the entire stream and can be resource intensive on the Server, Client and Network.")
		fmt.Println()
		ok, err := askConfirmation(fmt.Sprintf("Really detect gaps in stream %s with %s messages and %s bytes", c.stream, humanize.Comma(int64(info.State.Msgs)), humanize.IBytes(info.State.Bytes)), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	if c.json {
		c.showProgress = false
	}

	var progbar progress.Writer
	var tracker *progress.Tracker

	var gaps [][2]uint64
	var cnt int

	progressCb := func(seq uint64, pending uint64) {
		if !c.showProgress {
			return
		}
		if tracker == nil {
			progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
				Total: int64(pending),
			})
		}
		cnt++

		tracker.SetValue(int64(seq))

		if pending == 0 {
			tracker.SetValue(tracker.Total)
			tracker.MarkAsDone()
		}
	}

	gapCb := func(start, end uint64) {
		gaps = append(gaps, [2]uint64{start, end})
	}

	err = stream.DetectGaps(ctx, progressCb, gapCb)
	if tracker != nil {
		time.Sleep(250 * time.Millisecond) // let it draw
		progbar.Stop()
		fmt.Println()
	}
	if err != nil {
		return err
	}

	if c.json {
		iu.PrintJSON(gaps)
		return nil
	}

	if len(gaps) == 0 {
		fmt.Printf("No deleted messages in %s\n", c.stream)
		return nil
	}

	var table *iu.Table
	if len(gaps) == 1 {
		table = iu.NewTableWriter(opts(), fmt.Sprintf("1 gap found in Stream %s", c.stream))
	} else {
		table = iu.NewTableWriter(opts(), fmt.Sprintf("%s gaps found in Stream %s", f(len(gaps)), c.stream))
	}

	table.AddHeaders("First Message", "Last Message")
	for _, gap := range gaps {
		table.AddRow(f(gap[0]), f(gap[1]))
	}
	fmt.Println(table.Render())

	return nil
}

func (c *streamCmd) subjectsAction(_ *fisk.ParseContext) (err error) {
	asked := c.connectAndAskStream()

	subs, err := c.mgr.StreamContainedSubjects(c.stream, c.filterSubject)
	if err != nil {
		return err
	}

	if c.json {
		iu.PrintJSON(subs)
		return nil
	}

	if asked {
		fmt.Println()
	}

	if len(subs) == 0 {
		fmt.Printf("No subjects found matching %s\n", c.filterSubject)
		return nil
	}

	var longest int
	var most uint64
	var names []string

	for s, c := range subs {
		names = append(names, s)
		if len(s) > longest {
			longest = len(s)
		}
		if c > most {
			most = c
		}
	}

	cols := 1
	countWidth := len(f(most))
	table := iu.NewTableWriter(opts(), fmt.Sprintf("%d Subjects in stream %s", len(names), c.stream))

	switch {
	case longest+countWidth < 20:
		cols = 3
		table.AddHeaders("Subject", "Count", "Subject", "Count", "Subject", "Count")
	case longest+countWidth < 30:
		cols = 2
		table.AddHeaders("Subject", "Count", "Subject", "Count")
	default:
		table.AddHeaders("Subject", "Count")
	}

	sort.Slice(names, func(i, j int) bool {
		if c.reportSort == "name" || c.reportSort == "subjects" {
			return c.boolReverse(names[i] < names[j])
		} else {
			return c.boolReverse(subs[names[i]] < subs[names[j]])
		}
	})

	if c.listNames {
		for _, n := range names {
			fmt.Println(n)
		}
		return
	}

	comma := func(i uint64) string {
		if i == 0 {
			return ""
		}
		return f(i)
	}

	iu.SliceGroups(names, cols, func(g []string) {
		if cols == 1 {
			table.AddRow(g[0], comma(subs[g[0]]))
		} else if cols == 2 {
			table.AddRow(g[0], comma(subs[g[0]]), g[1], comma(subs[g[1]]))
		} else {
			table.AddRow(g[0], comma(subs[g[0]]), g[1], comma(subs[g[1]]), g[2], comma(subs[g[2]]))
		}
	})

	fmt.Println(table.Render())

	return nil
}

func (c *streamCmd) parseLimitStrings(_ *fisk.ParseContext) (err error) {
	if c.maxBytesLimitString != "" {
		c.maxBytesLimit, err = iu.ParseStringAsBytes(c.maxBytesLimitString)
		if err != nil {
			return err
		}
	}

	if c.maxMsgSizeString != "" {
		c.maxMsgSize, err = iu.ParseStringAsBytes(c.maxMsgSizeString)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *streamCmd) findAction(_ *fisk.ParseContext) (err error) {
	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	var opts []jsm.StreamQueryOpt
	if c.fServer != "" {
		opts = append(opts, jsm.StreamQueryServerName(c.fServer))
	}
	if c.fCluster != "" {
		opts = append(opts, jsm.StreamQueryClusterName(c.fCluster))
	}
	if c.fEmpty {
		opts = append(opts, jsm.StreamQueryWithoutMessages())
	}
	if c.fIdle > 0 {
		opts = append(opts, jsm.StreamQueryIdleLongerThan(c.fIdle))
	}
	if c.fCreated > 0 {
		opts = append(opts, jsm.StreamQueryOlderThan(c.fCreated))
	}
	if c.fConsumers >= 0 {
		opts = append(opts, jsm.StreamQueryFewerConsumersThan(uint(c.fConsumers)))
	}
	if c.fInvert {
		opts = append(opts, jsm.StreamQueryInvert())
	}
	if c.filterSubject != "" {
		opts = append(opts, jsm.StreamQuerySubjectWildcard(c.filterSubject))
	}
	if c.fSourcedSet {
		opts = append(opts, jsm.StreamQueryIsSourced())
	}
	if c.fMirroredSet {
		opts = append(opts, jsm.StreamQueryIsMirror())
	}
	if c.fReplicas > 0 {
		opts = append(opts, jsm.StreamQueryReplicas(c.fReplicas))
	}
	if c.fExpression != "" {
		opts = append(opts, jsm.StreamQueryExpression(c.fExpression))
	}
	if c.fLeader != "" {
		opts = append(opts, jsm.StreamQueryLeaderServer(c.fLeader))
	}

	found, err := c.mgr.QueryStreams(opts...)
	if err != nil {
		return err
	}

	out := ""
	switch {
	case c.listNames:
		out = c.renderStreamsAsList(found, nil)
	default:
		out, err = c.renderStreamsAsTable(found, nil)
	}
	if err != nil {
		return err
	}

	fmt.Println(out)

	return nil
}

func (c *streamCmd) loadStream(stream string) (*jsm.Stream, error) {
	if c.selectedStream != nil && c.selectedStream.Name() == stream {
		return c.selectedStream, nil
	}

	return c.mgr.LoadStream(stream)
}

func (c *streamCmd) balanceAction(_ *fisk.ParseContext) error {
	var err error

	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	if err != nil {
		return fmt.Errorf("setup failed: %v", err)
	}

	var opts []jsm.StreamQueryOpt
	if c.fServer != "" {
		opts = append(opts, jsm.StreamQueryServerName(c.fServer))
	}
	if c.fCluster != "" {
		opts = append(opts, jsm.StreamQueryClusterName(c.fCluster))
	}
	if c.fEmpty {
		opts = append(opts, jsm.StreamQueryWithoutMessages())
	}
	if c.fIdle > 0 {
		opts = append(opts, jsm.StreamQueryIdleLongerThan(c.fIdle))
	}
	if c.fCreated > 0 {
		opts = append(opts, jsm.StreamQueryOlderThan(c.fCreated))
	}
	if c.fConsumers >= 0 {
		opts = append(opts, jsm.StreamQueryFewerConsumersThan(uint(c.fConsumers)))
	}
	if c.fInvert {
		opts = append(opts, jsm.StreamQueryInvert())
	}
	if c.filterSubject != "" {
		opts = append(opts, jsm.StreamQuerySubjectWildcard(c.filterSubject))
	}
	if c.fSourcedSet {
		opts = append(opts, jsm.StreamQueryIsSourced())
	}
	if c.fMirroredSet {
		opts = append(opts, jsm.StreamQueryIsMirror())
	}
	if c.fReplicas > 0 {
		opts = append(opts, jsm.StreamQueryReplicas(c.fReplicas))
	}
	if c.fExpression != "" {
		opts = append(opts, jsm.StreamQueryExpression(c.fExpression))
	}
	if c.fLeader != "" {
		opts = append(opts, jsm.StreamQueryLeaderServer(c.fLeader))
	}

	found, err := c.mgr.QueryStreams(opts...)
	if err != nil {
		return err
	}

	if len(found) > 0 {
		balancer, err := balancer.New(c.mgr.NatsConn(), api.NewDefaultLogger(api.InfoLevel))
		if err != nil {
			return err
		}

		balanced, err := balancer.BalanceStreams(found)
		if err != nil {
			return fmt.Errorf("failed to balance streams: %s", err)
		}
		fmt.Printf("Balanced %d streams.\n", balanced)

	}

	return nil
}

func (c *streamCmd) leaderStandDown(_ *fisk.ParseContext) error {
	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	if info.Cluster == nil || len(info.Cluster.Replicas) == 0 {
		return fmt.Errorf("stream %q is not clustered", stream.Name())
	}

	leader := info.Cluster.Leader
	if leader == "" && !c.force {
		return fmt.Errorf("stream %q has no current leader", stream.Name())
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

	log.Printf("Requesting leader step down of %q for stream %q in a %d peer cluster group", leader, stream.Name(), len(info.Cluster.Replicas)+1)
	err = stream.LeaderStepDown(p)
	if err != nil {
		return err
	}

	ctr := 0
	start := time.Now()
	for range time.NewTicker(500 * time.Millisecond).C {
		if ctr == 10 {
			return fmt.Errorf("stream %q did not elect a new leader in time", stream.Name())
		}
		ctr++

		info, err = stream.Information()
		if err != nil {
			log.Printf("Failed to retrieve Stream State: %s", err)
			continue
		}

		if info.Cluster == nil || info.Cluster.Leader == "" {
			log.Printf("No leader elected")
			continue
		}

		if info.Cluster.Leader != leader {
			log.Printf("New leader elected %q", info.Cluster.Leader)
			break
		}
	}

	if info.Cluster.Leader == leader {
		log.Printf("Leader did not change after %s", time.Since(start).Round(time.Millisecond))
	}

	fmt.Println()
	return c.showStream(stream)
}

func (c *streamCmd) removePeer(_ *fisk.ParseContext) error {
	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	info, err := stream.Information()
	if err != nil {
		return err
	}

	if info.Cluster == nil {
		return fmt.Errorf("stream %q is not clustered", stream.Name())
	}

	peerNames := []string{info.Cluster.Leader}
	for _, r := range info.Cluster.Replicas {
		peerNames = append(peerNames, r.Name)
	}

	if len(peerNames) == 1 && !c.force {
		return fmt.Errorf("removing the only peer on a stream will result in data loss, use --force to force")
	}

	if c.peerName == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Select a Peer",
			Options: peerNames,
		}, &c.peerName)
		if err != nil {
			return err
		}
	}

	log.Printf("Removing peer %q", c.peerName)

	err = stream.RemoveRAFTPeer(c.peerName)
	if err != nil {
		return err
	}

	log.Printf("Requested removal of peer %q", c.peerName)

	return nil
}

func (c *streamCmd) viewAction(_ *fisk.ParseContext) error {
	if !iu.IsTerminal() {
		return fmt.Errorf("interactive stream paging requires a valid terminal")
	}

	if c.vwPageSize > 25 {
		c.vwPageSize = 25
	}

	c.connectAndAskStream()

	str, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	pops := []jsm.PagerOption{
		jsm.PagerSize(c.vwPageSize),
	}

	switch {
	case c.vwStartDelta > 0:
		pops = append(pops, jsm.PagerStartDelta(c.vwStartDelta))
	case c.vwStartId > 0:
		pops = append(pops, jsm.PagerStartId(c.vwStartId))
	}

	if c.vwSubject != "" {
		pops = append(pops, jsm.PagerFilterSubject(c.vwSubject))
	}

	pgr, err := str.PageContents(pops...)
	if err != nil {
		return err
	}
	defer pgr.Close()

	ctx, cancel := context.WithCancel(ctx)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-sigs:
			cancel()
		}
	}()

	shouldTerminate := false

	for {
		msg, last, err := pgr.NextMsg(ctx)
		if err != nil {
			if !last {
				return err
			}
			// later we know we reached final last after showing the final message
			shouldTerminate = true
		}

		switch {
		case msg == nil:
			shouldTerminate = true
		case c.vwRaw:
			fmt.Println(string(msg.Data))
		default:
			meta, err := jsm.ParseJSMsgMetadata(msg)
			if err == nil {
				fmt.Printf("[%d] Subject: %s Received: %s\n", meta.StreamSequence(), msg.Subject, meta.TimeStamp().Format(time.RFC3339))
			} else {
				fmt.Printf("Subject: %s Reply: %s\n", msg.Subject, msg.Reply)
			}

			if len(msg.Header) > 0 {
				fmt.Println()
				for k, vs := range msg.Header {
					for _, v := range vs {
						if k == "Nats-Stream-Source" {
							v = strings.ReplaceAll(v, "\f", "\u240A")
						}

						if k == "Nats-Subject" || k == "Nats-Stream" || k == "Nats-Sequence" || k == "Nats-Time-Stamp" || k == "Nats-Num-Pending" || k == "Nats-Last-Sequence" || k == "Nats-UpTo-Sequnce" {
							continue
						}

						fmt.Printf("  %s: %s\n", k, v)
					}
				}
			}

			outPutMSGBody(msg.Data, c.vwTranslate, msg.Subject, meta.Stream())
		}

		if shouldTerminate {
			log.Println("Reached apparent end of data")
			return nil
		}

		if last {
			next := false
			iu.AskOne(&survey.Confirm{Message: "Next Page?", Default: true}, &next)
			if !next {
				return nil
			}
		}
	}
}

func (c *streamCmd) sealAction(_ *fisk.ParseContext) error {
	c.connectAndAskStream()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really seal Stream %s, sealed streams can not be unsealed or modified", c.stream), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	stream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not seal Stream")

	stream.Seal()
	fisk.FatalIfError(err, "could not seal Stream")

	return c.showStream(stream)
}

func (c *streamCmd) restoreAction(_ *fisk.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")

	var bm api.JSApiStreamRestoreRequest
	bmj, err := os.ReadFile(filepath.Join(c.backupDirectory, "backup.json"))
	fisk.FatalIfError(err, "restore failed")
	err = json.Unmarshal(bmj, &bm)
	fisk.FatalIfError(err, "restore failed")

	var cfg *api.StreamConfig

	known, err := mgr.IsKnownStream(bm.Config.Name)
	fisk.FatalIfError(err, "Could not check if the stream already exist")
	if known {
		fisk.Fatalf("Stream %q already exist", bm.Config.Name)
	}

	var progbar progress.Writer
	var tracker *progress.Tracker
	var prevMsg time.Time

	cb := func(p jsm.RestoreProgress) {
		if opts().Trace && (p.ChunksSent()%100 == 0 || time.Since(prevMsg) > 500*time.Millisecond) {
			fmt.Printf("Sent %v chunk %v / %v at %v / s\n", fiBytes(uint64(p.ChunkSize())), p.ChunksSent(), p.ChunksToSend(), fiBytes(p.BytesPerSecond()))
			return
		}

		prevMsg = time.Now()

		if progbar == nil {
			progbar, tracker, _ = iu.NewProgress(opts(), &progress.Tracker{
				Total: int64(p.ChunksToSend() * p.ChunkSize()),
				Units: progress.UnitsBytes,
			})
		}

		tracker.SetValue(int64(p.ChunksSent() * uint32(p.ChunkSize())))
	}

	var ropts []jsm.SnapshotOption

	if c.showProgress {
		ropts = append(ropts, jsm.RestoreNotify(cb))
	} else {
		ropts = append(ropts, jsm.SnapshotDebug())
	}

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		if err != nil {
			return err
		}

		// we need to confirm this new config has the same stream
		// name as the snapshot else the server state can get confused
		// see https://github.com/nats-io/nats-server/issues/2850
		if bm.Config.Name != cfg.Name {
			return fmt.Errorf("stream names may not be changed during restore")
		}
	} else {
		cfg = &bm.Config
	}

	if c.placementCluster != "" || len(c.placementTags) > 0 {
		cfg.Placement = &api.Placement{
			Cluster: c.placementCluster,
			Tags:    c.placementTags,
		}
	}

	if c.replicas > 0 {
		cfg.Replicas = int(c.replicas)
	}

	if cfg != nil {
		ropts = append(ropts, jsm.RestoreConfiguration(*cfg))
	}

	fmt.Printf("Starting restore of Stream %q from file %q\n\n", bm.Config.Name, c.backupDirectory)

	fp, _, err := mgr.RestoreSnapshotFromDirectory(ctx, bm.Config.Name, c.backupDirectory, ropts...)
	fisk.FatalIfError(err, "restore failed")
	if c.showProgress {
		tracker.SetValue(int64(fp.ChunksSent() * uint32(fp.ChunkSize())))
		time.Sleep(300 * time.Millisecond)
		progbar.Stop()
	}

	fmt.Println()
	fmt.Printf("Restored stream %q in %v\n", bm.Config.Name, fp.EndTime().Sub(fp.StartTime()).Round(time.Second))
	fmt.Println()

	stream, err := mgr.LoadStream(bm.Config.Name)
	fisk.FatalIfError(err, "could not request Stream info")
	err = c.showStream(stream)
	fisk.FatalIfError(err, "could not show stream")

	return nil
}

func backupStream(stream *jsm.Stream, showProgress bool, consumers bool, check bool, target string, chunkSize int) error {
	first := true
	pmu := sync.Mutex{}
	expected := 1
	timedOut := false

	var progbar progress.Writer
	var tracker *progress.Tracker
	var err error
	var prevMsg time.Time

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	idleTimeout := 5 * time.Second
	if opts().Timeout > idleTimeout {
		idleTimeout = opts().Timeout
	}

	timeout := time.AfterFunc(idleTimeout, func() {
		cancel()
		timedOut = true
	})

	var received uint32

	cb := func(p jsm.SnapshotProgress) {
		if tracker == nil && showProgress {
			if p.BytesExpected() > 0 {
				expected = int(p.BytesExpected())
			}
			progbar, tracker, err = iu.NewProgress(opts(), &progress.Tracker{
				Total: int64(expected),
				Units: iu.ProgressUnitsIBytes,
			})
		}

		if first {
			fmt.Printf("Starting backup of Stream %q with %s\n", stream.Name(), humanize.IBytes(p.BytesExpected()))
			if showProgress {
				fmt.Println()
			}

			if p.HealthCheck() {
				fmt.Printf("Health Check was requested, this can take a long time without progress reports\n\n")
			}

			first = false
		}

		if opts().Trace {
			if first {
				fmt.Printf("Received %s chunk %s\n", fiBytes(uint64(p.ChunkSize())), f(p.ChunksReceived()))
			} else {
				fmt.Printf("Received %s chunk %s with time delta %s\n", fiBytes(uint64(p.ChunkSize())), f(p.ChunksReceived()), time.Since(prevMsg))
			}
		}

		if p.ChunksReceived() != received {
			timeout.Reset(idleTimeout)
			received = p.ChunksReceived()
		}

		if tracker != nil {
			tracker.SetValue(int64(p.UncompressedBytesReceived()))
		}

		prevMsg = time.Now()
	}

	sopts := []jsm.SnapshotOption{
		jsm.SnapshotChunkSize(chunkSize),
		jsm.SnapshotNotify(cb),
	}

	if consumers {
		sopts = append(sopts, jsm.SnapshotConsumers())
	}

	if opts().Trace {
		sopts = append(sopts, jsm.SnapshotDebug())
		showProgress = false
	}

	if check {
		sopts = append(sopts, jsm.SnapshotHealthCheck())
	}

	fp, err := stream.SnapshotToDirectory(ctx, target, sopts...)
	if err != nil {
		return err
	}

	pmu.Lock()
	if tracker != nil {
		tracker.SetValue(int64(expected))
		tracker.MarkAsDone()
		time.Sleep(300 * time.Millisecond)
		progbar.Stop()
	}
	pmu.Unlock()

	fmt.Println()

	if timedOut {
		return fmt.Errorf("backup timed out after receiving no data for a long period")
	}

	fmt.Printf("Received %s compressed data in %s chunks for stream %q in %v, %s uncompressed \n", humanize.IBytes(fp.BytesReceived()), f(fp.ChunksReceived()), stream.Name(), fp.EndTime().Sub(fp.StartTime()).Round(time.Millisecond), fiBytes(fp.UncompressedBytesReceived()))

	return nil
}

func (c *streamCmd) backupAction(_ *fisk.ParseContext) error {
	var err error

	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")

	stream, err := c.loadStream(c.stream)
	if err != nil {
		return err
	}

	chunkSize := int64(128 * 1024)
	if c.chunkSize != "" {
		chunkSize, err = iu.ParseStringAsBytes(c.chunkSize)
		if err != nil {
			return err
		}
	}

	err = backupStream(stream, c.showProgress, c.snapShotConsumers, c.healthCheck, c.backupDirectory, int(chunkSize))
	fisk.FatalIfError(err, "snapshot failed")

	return nil
}

func (c *streamCmd) reportAction(_ *fisk.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")

	if !c.json {
		fmt.Print("Obtaining Stream stats\n\n")
	}

	stats := []streamStat{}
	leaders := make(map[string]*raftLeader)
	showReplication := false
	var filter *jsm.StreamNamesFilter

	if c.filterSubject != "" {
		filter = &jsm.StreamNamesFilter{Subject: c.filterSubject}
	}

	dg := dot.NewGraph(dot.Directed)
	dg.Label("Stream Replication Structure")

	missing, err := mgr.EachStream(filter, func(stream *jsm.Stream) {
		info, err := stream.LatestInformation()
		fisk.FatalIfError(err, "could not get stream info for %s", stream.Name())

		if info.Cluster != nil {
			if c.reportLimitCluster != "" && info.Cluster.Name != c.reportLimitCluster {
				return
			}

			if info.Cluster.Leader != "" {
				_, ok := leaders[info.Cluster.Leader]
				if !ok {
					leaders[info.Cluster.Leader] = &raftLeader{name: info.Cluster.Leader, cluster: info.Cluster.Name}
				}
				leaders[info.Cluster.Leader].groups++
			}
		}

		deleted := info.State.NumDeleted
		// backward compat with servers that predate the num_deleted response
		if len(info.State.Deleted) > 0 {
			deleted = len(info.State.Deleted)
		}

		s := streamStat{
			Name:      info.Config.Name,
			Consumers: info.State.Consumers,
			Msgs:      int64(info.State.Msgs),
			Bytes:     info.State.Bytes,
			Storage:   info.Config.Storage.String(),
			Template:  info.Config.Template,
			Cluster:   info.Cluster,
			Deleted:   deleted,
			Mirror:    info.Mirror,
			Sources:   info.Sources,
			Placement: info.Config.Placement,
		}

		if info.State.Lost != nil {
			s.LostBytes = info.State.Lost.Bytes
			s.LostMsgs = len(info.State.Lost.Msgs)
		}

		if len(info.Config.Sources) > 0 {
			showReplication = true
			node, ok := dg.FindNodeById(info.Config.Name)
			if !ok {
				node = dg.Node(info.Config.Name)
			}
			for _, source := range info.Config.Sources {
				snode, ok := dg.FindNodeById(source.Name)
				if !ok {
					snode = dg.Node(source.Name)
				}
				edge := dg.Edge(snode, node).Attr("color", "green")

				if source.FilterSubject == "" {
					continue
				}

				if len(source.SubjectTransforms) == 0 {
					edge.Label(source.FilterSubject)
				} else if len(source.SubjectTransforms) == 1 {
					edge.Label(source.SubjectTransforms[0].Source + " to " + source.SubjectTransforms[0].Destination)
				}
			}
		}

		if info.Config.Mirror != nil {
			showReplication = true
			node, ok := dg.FindNodeById(info.Config.Name)
			if !ok {
				node = dg.Node(info.Config.Name)
			}
			mnode, ok := dg.FindNodeById(info.Config.Mirror.Name)
			if !ok {
				mnode = dg.Node(info.Config.Mirror.Name)
			}
			dg.Edge(mnode, node).Attr("color", "blue").Label("Mirror")
		}

		stats = append(stats, s)
	})
	if err != nil {
		return err
	}

	if len(stats) == 0 {
		if !c.json {
			fmt.Println("No Streams defined")
		}
		return nil
	}

	if c.reportSortConsumers {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Consumers < stats[j].Consumers })
	} else if c.reportSortMsgs {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Msgs < stats[j].Msgs })
	} else if c.reportSortName {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Name < stats[j].Name })
	} else if c.reportSortStorage {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Storage < stats[j].Storage })
	} else {
		sort.Slice(stats, func(i, j int) bool { return stats[i].Bytes < stats[j].Bytes })
	}

	c.renderStreams(stats)

	if showReplication {
		c.renderReplication(stats)

		if c.outFile != "" {
			os.WriteFile(c.outFile, []byte(dg.String()), 0600)
		}
	}

	if c.reportLeaderDistrib && len(leaders) > 0 {
		renderRaftLeaders(leaders, "Streams")
	}

	c.renderMissing(os.Stdout, missing)

	return nil
}

func (c *streamCmd) renderReplication(stats []streamStat) {
	table := iu.NewTableWriter(opts(), "Replication Report")
	table.AddHeaders("Stream", "Kind", "API Prefix", "Source Stream", "Filters and Transforms", "Active", "Lag", "Error")

	for _, s := range stats {
		if len(s.Sources) == 0 && s.Mirror == nil {
			continue
		}

		if s.Mirror != nil {
			apierr := ""
			if s.Mirror.Error != nil {
				apierr = s.Mirror.Error.Error()
			}

			eApiPrefix := ""
			if s.Mirror.External != nil {
				eApiPrefix = s.Mirror.External.ApiPrefix
			}

			if c.reportRaw {
				table.AddRow(s.Name, "Mirror", eApiPrefix, s.Mirror.Name, "", s.Mirror.Active, s.Mirror.Lag, apierr)
			} else {
				table.AddRow(s.Name, "Mirror", eApiPrefix, s.Mirror.Name, "", f(s.Mirror.Active), f(s.Mirror.Lag), apierr)
			}
		}

		for _, source := range s.Sources {
			apierr := ""
			if source != nil && source.Error != nil {
				apierr = source.Error.Error()
			}

			eApiPrefix := ""
			if source.External != nil {
				eApiPrefix = source.External.ApiPrefix
			}

			filterSubject := []string{}

			for _, transform := range source.SubjectTransforms {
				filterSubject = append(filterSubject, fmt.Sprintf("%s to %s", transform.Source, transform.Destination))
			}

			if len(filterSubject) == 0 && source.FilterSubject != "" {
				filterSubject = append(filterSubject, fmt.Sprintf("%s untransformed", source.FilterSubject))
			}

			if c.reportRaw {
				table.AddRow(s.Name, "Source", eApiPrefix, source.Name, strings.Join(filterSubject, ", "), source.Active, source.Lag, apierr)
			} else {
				table.AddRow(s.Name, "Source", eApiPrefix, source.Name, strings.Join(filterSubject, ", "), f(source.Active), f(source.Lag), apierr)
			}

		}
	}
	fmt.Println(table.Render())
}

func (c *streamCmd) renderStreams(stats []streamStat) {
	table := iu.NewTableWriter(opts(), "Stream Report")
	table.AddHeaders("Stream", "Storage", "Placement", "Consumers", "Messages", "Bytes", "Lost", "Deleted", "Replicas")

	for _, s := range stats {
		lost := "0"
		placement := ""
		if s.Placement != nil {
			if s.Placement.Cluster != "" {
				placement = fmt.Sprintf("cluster: %s ", s.Placement.Cluster)
			}
			if len(s.Placement.Tags) > 0 {
				placement = fmt.Sprintf("%stags: %s", placement, f(s.Placement.Tags))
			}
		}

		if c.reportRaw {
			if s.LostMsgs > 0 {
				lost = fmt.Sprintf("%d (%d)", s.LostMsgs, s.LostBytes)
			}
			table.AddRow(s.Name, s.Storage, placement, s.Consumers, s.Msgs, s.Bytes, lost, s.Deleted, renderCluster(s.Cluster))
		} else {
			if s.LostMsgs > 0 {
				lost = fmt.Sprintf("%s (%s)", f(s.LostMsgs), humanize.IBytes(s.LostBytes))
			}
			table.AddRow(s.Name, s.Storage, placement, f(s.Consumers), f(s.Msgs), humanize.IBytes(s.Bytes), lost, f(s.Deleted), renderCluster(s.Cluster))
		}
	}

	fmt.Println(table.Render())
}

func (c *streamCmd) loadConfigFile(file string) (*api.StreamConfig, error) {
	f, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var cfg api.StreamConfig

	// there is a chance that this is a `nats s info --json` output
	// which is a StreamInfo, so we detect if this is one of those
	// by checking if there's a config key then extract that, else
	// we try loading it as a StreamConfig

	var nfo map[string]any
	err = json.Unmarshal(f, &nfo)
	if err != nil {
		return nil, err
	}

	_, ok := nfo["config"]
	if ok {
		var nfo api.StreamInfo
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

	if cfg.Name != c.stream && c.stream != "" {
		cfg.Name = c.stream
	}

	return &cfg, nil
}

func (c *streamCmd) checkRepubTransform() {
	if (c.repubSource != "" && c.repubDest == "") || (c.repubSource == "" && c.repubDest != "") || (c.repubHeadersOnly && (c.repubSource == "" || c.repubDest == "")) {
		msg := "must specify both --republish-source and --republish-destination"

		if c.repubHeadersOnly {
			msg = msg + " when using --headers-only"
		}

		fisk.Fatalf(msg)
	}

	if (c.subjectTransformSource != "" && c.subjectTransformDest == "") || (c.subjectTransformSource == "" && c.subjectTransformDest != "") {
		msg := "must specify both --transform-source and --transform-destination"

		if c.repubHeadersOnly {
			msg = msg + " when using --headers-only"
		}

		fisk.Fatalf(msg)
	}
}

func (c *streamCmd) copyAndEditStream(cfg api.StreamConfig, pc *fisk.ParseContext) (api.StreamConfig, error) {
	var err error

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		if err != nil {
			return api.StreamConfig{}, err
		}

		if cfg.Name == "" {
			cfg.Name = c.stream
		}

		return *cfg, nil
	}

	c.checkRepubTransform()

	cfg.NoAck = !c.ack

	if c.discardPolicy != "" {
		cfg.Discard = c.discardPolicyFromString()
	}

	if len(c.subjects) > 0 {
		cfg.Subjects = iu.SplitCLISubjects(c.subjects)
	}

	if c.storage != "" {
		cfg.Storage = c.storeTypeFromString(c.storage)
	}

	if c.retentionPolicyS != "" {
		cfg.Retention = c.retentionPolicyFromString()
	}

	if c.maxBytesLimit != 0 {
		cfg.MaxBytes = c.maxBytesLimit
	}

	if c.maxMsgLimit != 0 {
		cfg.MaxMsgs = c.maxMsgLimit
	}

	if c.maxMsgPerSubjectLimit != 0 {
		cfg.MaxMsgsPer = c.maxMsgPerSubjectLimit
	}

	if c.maxAgeLimit != "" {
		cfg.MaxAge, err = fisk.ParseDuration(c.maxAgeLimit)
		if err != nil {
			return api.StreamConfig{}, fmt.Errorf("invalid maximum age limit format: %v", err)
		}
	}

	if c.maxMsgSize != 0 {
		cfg.MaxMsgSize = int32(c.maxMsgSize)
	}

	if c.maxConsumers != -1 {
		cfg.MaxConsumers = c.maxConsumers
	}

	if c.dupeWindow != "" {
		dw, err := fisk.ParseDuration(c.dupeWindow)
		if err != nil {
			return api.StreamConfig{}, fmt.Errorf("invalid duplicate window: %v", err)
		}
		cfg.Duplicates = dw
	}

	if c.replicas != 0 {
		cfg.Replicas = int(c.replicas)
	}

	if cfg.Placement == nil {
		cfg.Placement = &api.Placement{}
	}

	// For placement constraints, we explicitly support empty strings to
	// remove, so use the *Set bool variables to distinguish "was set on
	// command-line" from "is not empty".

	if c.placementClusterSet {
		cfg.Placement.Cluster = c.placementCluster
	}

	if c.placementTagsSet {
		// With the repeated set, we do accumulate the empty string as a list item.
		// We do still need the separate IsSetByUser variable to get that.
		if len(c.placementTags) == 0 || (len(c.placementTags) == 1 && c.placementTags[0] == "") {
			cfg.Placement.Tags = nil
		} else {
			cfg.Placement.Tags = c.placementTags
		}
	}

	if cfg.Placement.Cluster == "" && len(cfg.Placement.Tags) == 0 {
		cfg.Placement = nil
	}

	if len(c.sources) > 0 || c.mirror != "" {
		return cfg, fmt.Errorf("cannot edit mirrors, or sources using the CLI, use --config instead")
	}

	if c.description != "" {
		cfg.Description = c.description
	}

	if c.allowRollupSet {
		cfg.RollupAllowed = c.allowRollup
	}

	if c.denyPurgeSet {
		cfg.DenyPurge = c.denyPurge
	}

	if c.denyDeleteSet {
		cfg.DenyDelete = c.denyDelete
	}

	if c.allowDirectSet {
		cfg.AllowDirect = c.allowDirect
	}

	if c.allowMirrorDirectSet {
		cfg.MirrorDirect = c.allowMirrorDirect
	}

	if c.allowMsgTTL {
		cfg.AllowMsgTTL = c.allowMsgTTL
	}

	if c.discardPerSubjSet {
		cfg.DiscardNewPer = c.discardPerSubj
	}

	if c.metadataIsSet {
		cfg.Metadata = c.metadata
	}

	if c.compressionSet {
		if err = cfg.Compression.UnmarshalJSON([]byte(fmt.Sprintf("%q", c.compression))); err != nil {
			return cfg, fmt.Errorf("invalid compression algorithm")
		}
	}

	if !c.noRepub && c.repubSource != "" && c.repubDest != "" {
		cfg.RePublish = &api.RePublish{
			Source:      c.repubSource,
			Destination: c.repubDest,
			HeadersOnly: c.repubHeadersOnly,
		}
	}

	if c.noSubjectTransform {
		cfg.SubjectTransform = nil
	} else {
		var subjectTransformConfig api.SubjectTransformConfig

		if cfg.SubjectTransform != nil {
			subjectTransformConfig = *cfg.SubjectTransform
		}

		subjectTransformConfig.Source = c.subjectTransformSource
		subjectTransformConfig.Destination = c.subjectTransformDest

		if subjectTransformConfig.Source != "" && subjectTransformConfig.Destination != "" {
			cfg.SubjectTransform = &subjectTransformConfig
		}
	}

	if c.subjectDeleteMarkerTTLSet {
		cfg.SubjectDeleteMarkerTTL = c.subjectDeleteMarkerTTL
	}

	return cfg, nil
}

func (c *streamCmd) interactiveEdit(cfg api.StreamConfig) (api.StreamConfig, error) {
	cj, err := decoratedYamlMarshal(cfg)
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile, err := os.CreateTemp("", "*.yaml")
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}
	defer os.Remove(tfile.Name())

	_, err = fmt.Fprint(tfile, string(cj))
	if err != nil {
		return api.StreamConfig{}, fmt.Errorf("could not create temporary file: %s", err)
	}

	tfile.Close()

	err = iu.EditFile(tfile.Name())
	if err != nil {
		return api.StreamConfig{}, err
	}

	nb, err := os.ReadFile(tfile.Name())
	if err != nil {
		return api.StreamConfig{}, err
	}

	ncfg := api.StreamConfig{}
	err = yaml.Unmarshal(nb, &ncfg)
	if err != nil {
		return api.StreamConfig{}, err
	}

	// some yaml quirks
	if len(ncfg.Sources) == 0 {
		ncfg.Sources = nil
	}
	if len(ncfg.Metadata) == 0 {
		ncfg.Metadata = nil
	}

	return ncfg, nil
}

func (c *streamCmd) editAction(pc *fisk.ParseContext) error {
	c.connectAndAskStream()

	sourceStream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	// lazy deep copy
	input := sourceStream.Configuration()
	input.Metadata = iu.RemoveReservedMetadata(input.Metadata)

	ij, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var cfg api.StreamConfig
	err = json.Unmarshal(ij, &cfg)
	if err != nil {
		return err
	}

	if c.interactive {
		cfg, err = c.interactiveEdit(cfg)
		fisk.FatalIfError(err, "could not create new configuration for Stream %s", c.stream)
	} else {
		cfg, err = c.copyAndEditStream(cfg, pc)
		fisk.FatalIfError(err, "could not create new configuration for Stream %s", c.stream)
	}

	// sorts strings to subject lists that only differ in ordering is considered equal
	sorter := cmp.Transformer("Sort", func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	})

	diff := cmp.Diff(input, cfg, sorter)
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

	if jsm.IsKVBucketStream(c.stream) {
		err := c.kvAbstractionWarn(c.stream, "Really operate on the KV stream?")
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really edit Stream %s", c.stream), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	err = sourceStream.UpdateConfiguration(cfg)
	fisk.FatalIfError(err, "could not edit Stream %s", c.stream)

	if !c.json {
		fmt.Printf("Stream %s was updated\n\n", c.stream)
	}

	return c.showStream(sourceStream)
}

func (c *streamCmd) cpAction(pc *fisk.ParseContext) error {
	if c.stream == c.destination {
		fisk.Fatalf("source and destination Stream names cannot be the same")
	}

	c.connectAndAskStream()

	sourceStream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not request Stream %s configuration", c.stream)

	// lazy deep copy
	input := sourceStream.Configuration()
	ij, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var cfg api.StreamConfig
	err = json.Unmarshal(ij, &cfg)
	if err != nil {
		return err
	}

	cfg, err = c.copyAndEditStream(cfg, pc)
	fisk.FatalIfError(err, "could not copy Stream %s", c.stream)

	cfg.Name = c.destination

	newStream, err := c.mgr.NewStreamFromDefault(cfg.Name, cfg)
	fisk.FatalIfError(err, "could not create Stream")

	if !c.json {
		fmt.Printf("Stream %s was created\n\n", c.stream)
	}

	c.showStream(newStream)

	return nil
}

func (c *streamCmd) showStreamConfig(cols *columns.Writer, cfg api.StreamConfig) {
	cols.AddRowIfNotEmpty("Description", cfg.Description)
	cols.AddRowIf("Subjects", cfg.Subjects, len(cfg.Subjects) > 0)
	cols.AddRow("Replicas", cfg.Replicas)
	cols.AddRowIf("Sealed", true, cfg.Sealed)
	cols.AddRow("Storage", cfg.Storage.String())
	cols.AddRowIf("Compression", cfg.Compression, cfg.Compression != api.NoCompression)

	if cfg.FirstSeq > 0 {
		cols.AddRow("First Sequence", cfg.FirstSeq)
	}

	if cfg.Placement != nil {
		cols.AddRowIfNotEmpty("Placement Cluster", cfg.Placement.Cluster)
		cols.AddRowIf("Placement Tags", cfg.Placement.Tags, len(cfg.Placement.Tags) > 0)
	}

	cols.AddSectionTitle("Options")

	if cfg.SubjectTransform != nil && cfg.SubjectTransform.Destination != "" {
		source := cfg.SubjectTransform.Source
		if source == "" {
			source = ">"
		}
		cols.AddRowf("Subject Transform", "%s to %s", source, cfg.SubjectTransform.Destination)
	}

	if cfg.RePublish != nil {
		if cfg.RePublish.HeadersOnly {
			cols.AddRowf("Republishing Headers", "%s to %s", cfg.RePublish.Source, cfg.RePublish.Destination)
		} else {
			cols.AddRowf("Republishing", "%s to %s", cfg.RePublish.Source, cfg.RePublish.Destination)
		}
	}
	cols.AddRow("Retention", cfg.Retention.String())
	cols.AddRow("Acknowledgments", !cfg.NoAck)
	dnp := cfg.Discard.String()
	if cfg.DiscardNewPer {
		dnp = "New Per Subject"
	}
	cols.AddRow("Discard Policy", dnp)
	cols.AddRow("Duplicate Window", cfg.Duplicates)
	cols.AddRowIf("Direct Get", cfg.AllowDirect, cfg.AllowDirect)
	cols.AddRowIf("Mirror Direct Get", cfg.MirrorDirect, cfg.MirrorDirect)
	cols.AddRow("Allows Msg Delete", !cfg.DenyDelete)
	cols.AddRow("Allows Purge", !cfg.DenyPurge)
	cols.AddRow("Allows Per-Message TTL", cfg.AllowMsgTTL)
	if cfg.AllowMsgTTL && cfg.SubjectDeleteMarkerTTL > 0 {
		cols.AddRow("Subject Delete Markers TTL", cfg.SubjectDeleteMarkerTTL)
	}
	cols.AddRow("Allows Rollups", cfg.RollupAllowed)

	cols.AddSectionTitle("Limits")
	if cfg.MaxMsgs == -1 {
		cols.AddRow("Maximum Messages", "unlimited")
	} else {
		cols.AddRow("Maximum Messages", cfg.MaxMsgs)
	}
	if cfg.MaxMsgsPer <= 0 {
		cols.AddRow("Maximum Per Subject", "unlimited")
	} else {
		cols.AddRow("Maximum Per Subject", cfg.MaxMsgsPer)
	}
	if cfg.MaxBytes == -1 {
		cols.AddRow("Maximum Bytes", "unlimited")
	} else {
		cols.AddRow("Maximum Bytes", humanize.IBytes(uint64(cfg.MaxBytes)))
	}
	if cfg.MaxAge <= 0 {
		cols.AddRow("Maximum Age", "unlimited")
	} else {
		cols.AddRow("Maximum Age", cfg.MaxAge)
	}
	if cfg.MaxMsgSize == -1 {
		cols.AddRow("Maximum Message Size", "unlimited")
	} else {
		cols.AddRow("Maximum Message Size", humanize.IBytes(uint64(cfg.MaxMsgSize)))
	}
	if cfg.MaxConsumers == -1 {
		cols.AddRow("Maximum Consumers", "unlimited")
	} else {
		cols.AddRow("Maximum Consumers", cfg.MaxConsumers)
	}
	cols.AddRowIf("Consumer Inactive Threshold", cfg.ConsumerLimits.InactiveThreshold, cfg.ConsumerLimits.InactiveThreshold > 0)
	cols.AddRowIf("Consumer Max Ack Pending", cfg.ConsumerLimits.MaxAckPending, cfg.ConsumerLimits.MaxAckPending > 0)

	meta := iu.RemoveReservedMetadata(cfg.Metadata)
	if len(meta) > 0 {
		cols.AddSectionTitle("Metadata")
		cols.AddMapStrings(meta)
	}

	if cfg.Mirror != nil || len(cfg.Sources) > 0 {
		cols.AddSectionTitle("Replication")
	}

	cols.AddRowIfNotEmpty("Mirror", c.renderSource(cfg.Mirror))

	if len(cfg.Sources) > 0 {
		sort.Slice(cfg.Sources, func(i, j int) bool {
			return cfg.Sources[i].Name < cfg.Sources[j].Name
		})

		for i, source := range cfg.Sources {
			l := ""
			if i == 0 {
				l = "Sources"
			}

			cols.AddRow(l, c.renderSource(source))
		}
	}

	cols.Println()
}

func (c *streamCmd) renderSource(s *api.StreamSource) string {
	if s == nil {
		return ""
	}

	var parts []string
	parts = append(parts, s.Name)

	if s.OptStartSeq > 0 {
		parts = append(parts, fmt.Sprintf("Start Seq: %s", f(s.OptStartSeq)))
	}

	if s.OptStartTime != nil {
		parts = append(parts, fmt.Sprintf("Start Time: %v", s.OptStartTime))
	}

	if s.External != nil {
		if s.External.ApiPrefix != "" {
			parts = append(parts, fmt.Sprintf("API Prefix: %s", s.External.ApiPrefix))
		}

		if s.External.DeliverPrefix != "" {
			parts = append(parts, fmt.Sprintf("Delivery Prefix: %s", s.External.DeliverPrefix))
		}
	}

	return f(parts)
}

func (c *streamCmd) showStream(stream *jsm.Stream) error {
	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	c.showStreamInfo(info)

	return nil
}

func (c *streamCmd) showStreamInfo(info *api.StreamInfo) {
	if c.json {
		err := iu.PrintJSON(info)
		fisk.FatalIfError(err, "could not display info")
		return
	}

	var cols *columns.Writer
	if c.showStateOnly {
		cols = newColumns(fmt.Sprintf("State for Stream %s created %s", c.stream, f(info.Created.Local())))
	} else {
		cols = newColumns(fmt.Sprintf("Information for Stream %s created %s", c.stream, f(info.Created.Local())))
		c.showStreamConfig(cols, info.Config)
	}

	if info.Cluster != nil && info.Cluster.Name != "" {
		cols.AddSectionTitle("Cluster Information")
		if info.Cluster != nil && info.Cluster.Name != "" {
			cols.AddRow("Name", info.Cluster.Name)
			cols.AddRowIfNotEmpty("Cluster Group", info.Cluster.RaftGroup)
			cols.AddRow("Leader", info.Cluster.Leader)
			for _, r := range info.Cluster.Replicas {
				state := []string{r.Name}

				if r.Current {
					state = append(state, "current")
				} else {
					state = append(state, "outdated")
				}

				if r.Offline {
					state = append(state, "OFFLINE")
				}

				if r.Active > 0 && r.Active < math.MaxInt64 {
					state = append(state, fmt.Sprintf("seen %s ago", f(r.Active)))
				} else {
					state = append(state, "not seen")
				}

				switch {
				case r.Lag > 1:
					state = append(state, fmt.Sprintf("%s operations behind", f(r.Lag)))
				case r.Lag == 1:
					state = append(state, fmt.Sprintf("%s operation behind", f(r.Lag)))
				}

				cols.AddRow("Replica", state)
			}
		}
		cols.Println()
	}

	showSource := func(s *api.StreamSourceInfo) {
		cols.AddRow("Stream Name", s.Name)

		switch {
		case s.FilterSubject != "":
			filter := ">"
			if s.FilterSubject != "" {
				filter = s.FilterSubject
			}

			cols.AddRowf("Subject Filter", filter)
		case len(s.SubjectTransforms) > 0:
			for i := range s.SubjectTransforms {
				t := ""

				if i == 0 {
					if len(s.SubjectTransforms) > 1 {
						t = "Subject Filters and Transforms"
					} else {
						t = "Subject Filter and Transform"
					}
				}

				if s.SubjectTransforms[i].Destination == "" {
					cols.AddRowf(t, "%s untransformed", s.SubjectTransforms[i].Source)
				} else {
					cols.AddRowf(t, "%s to %s", s.SubjectTransforms[i].Source, s.SubjectTransforms[i].Destination)
				}
			}
		}

		cols.AddRow("Lag", s.Lag)

		if s.Active > 0 && s.Active < math.MaxInt64 {
			cols.AddRow("Last Seen", s.Active)
		} else {
			cols.AddRow("Last Seen", "never")
		}

		if s.External != nil {
			cols.AddRow("Ext. API Prefix", s.External.ApiPrefix)
			if s.External.DeliverPrefix != "" {
				cols.AddRow("Ext. Delivery Prefix", s.External.DeliverPrefix)
			}
		}

		if s.Error != nil {
			cols.AddRow("Error", s.Error.Description)
		}
	}

	if info.Mirror != nil {
		cols.AddSectionTitle("Mirror Information")
		showSource(info.Mirror)
	}

	if len(info.Sources) > 0 {
		cols.AddSectionTitle("Source Information")
		for _, s := range info.Sources {
			showSource(s)
			cols.Println()
		}
	}

	cols.AddSectionTitle("State")
	iu.RenderMetaApi(cols, info.Config.Metadata)
	cols.AddRow("Messages", info.State.Msgs)
	cols.AddRow("Bytes", humanize.IBytes(info.State.Bytes))

	if info.State.Lost != nil && len(info.State.Lost.Msgs) > 0 {
		cols.AddRowf("Lost Messages", "%s (%s)", f(len(info.State.Lost.Msgs)), humanize.IBytes(info.State.Lost.Bytes))
	}

	if info.State.FirstTime.Equal(time.Unix(0, 0)) || info.State.FirstTime.IsZero() {
		cols.AddRow("First Sequence", info.State.FirstSeq)
	} else {
		cols.AddRowf("First Sequence", "%s @ %s", f(info.State.FirstSeq), f(info.State.FirstTime))
	}

	if info.State.LastTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		cols.AddRow("Last Sequence", info.State.LastSeq)
	} else {
		cols.AddRowf("Last Sequence", "%s @ %s", f(info.State.LastSeq), f(info.State.LastTime))
	}

	if len(info.State.Deleted) > 0 { // backwards compat with older servers
		cols.AddRow("Deleted Messages", len(info.State.Deleted))
	} else if info.State.NumDeleted > 0 {
		cols.AddRow("Deleted Messages", info.State.NumDeleted)
	}

	cols.AddRow("Active Consumers", info.State.Consumers)

	if info.State.NumSubjects > 0 {
		cols.AddRow("Number of Subjects", info.State.NumSubjects)
	}

	if len(info.Alternates) > 0 {
		lName := 0
		lCluster := 0
		for _, s := range info.Alternates {
			if len(s.Name) > lName {
				lName = len(s.Name)
			}
			if len(s.Cluster) > lCluster {
				lCluster = len(s.Cluster)
			}
		}

		for i, s := range info.Alternates {
			msg := fmt.Sprintf("%s%s: Cluster: %s%s", strings.Repeat(" ", lName-len(s.Name)), s.Name, strings.Repeat(" ", lCluster-len(s.Cluster)), s.Cluster)
			if s.Domain != "" {
				msg = fmt.Sprintf("%s Domain: %s", msg, s.Domain)
			}

			if i == 0 {
				cols.AddRow("Alternates", msg)
			} else {
				cols.AddRow("", msg)
			}
		}
	}

	cols.Frender(os.Stdout)
}

func (c *streamCmd) stateAction(pc *fisk.ParseContext) error {
	c.showStateOnly = true
	return c.infoAction(pc)
}

func (c *streamCmd) infoAction(_ *fisk.ParseContext) error {
	c.connectAndAskStream()

	stream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not request Stream info")
	err = c.showStream(stream)
	fisk.FatalIfError(err, "could not show stream")

	fmt.Println()

	return nil
}

func (c *streamCmd) discardPolicyFromString() api.DiscardPolicy {
	switch strings.ToLower(c.discardPolicy) {
	case "new":
		return api.DiscardNew
	case "old":
		return api.DiscardOld
	default:
		fisk.Fatalf("invalid discard policy %s", c.discardPolicy)
		return api.DiscardOld // unreachable
	}
}

func (c *streamCmd) storeTypeFromString(s string) api.StorageType {
	switch s {
	case "file", "f":
		return api.FileStorage
	case "memory", "m":
		return api.MemoryStorage
	default:
		fisk.Fatalf("invalid storage type %s", c.storage)
		return api.MemoryStorage // unreachable
	}
}

func (c *streamCmd) retentionPolicyFromString() api.RetentionPolicy {
	switch strings.ToLower(c.retentionPolicyS) {
	case "limits":
		return api.LimitsPolicy
	case "interest":
		return api.InterestPolicy
	case "work queue", "workq", "work":
		return api.WorkQueuePolicy
	default:
		fisk.Fatalf("invalid retention policy %s", c.retentionPolicyS)
		return api.LimitsPolicy // unreachable
	}
}

func (c *streamCmd) prepareConfig(_ *fisk.ParseContext, requireSize bool) api.StreamConfig {
	var err error

	if c.inputFile != "" {
		cfg, err := c.loadConfigFile(c.inputFile)
		fisk.FatalIfError(err, "invalid input")

		cfg.Metadata = iu.RemoveReservedMetadata(cfg.Metadata)

		if c.stream != "" {
			cfg.Name = c.stream
		}

		if c.stream == "" {
			c.stream = cfg.Name
		}

		if len(c.subjects) > 0 {
			cfg.Subjects = c.subjects
		}

		if len(c.placementTags) > 0 {
			if cfg.Placement == nil {
				cfg.Placement = &api.Placement{}
			}
			cfg.Placement.Tags = c.placementTags
		}

		if c.placementCluster != "" {
			if cfg.Placement == nil {
				cfg.Placement = &api.Placement{}
			}
			cfg.Placement.Cluster = c.placementCluster
		}

		if c.description != "" {
			cfg.Description = c.description
		}

		if c.replicas > 0 {
			cfg.Replicas = int(c.replicas)
		}

		return *cfg
	}

	if c.stream == "" {
		err = iu.AskOne(&survey.Input{
			Message: "Stream Name",
		}, &c.stream, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "invalid input")
	}

	if c.mirror == "" && len(c.sources) == 0 {
		if len(c.subjects) == 0 {
			subjects := ""
			err = iu.AskOne(&survey.Input{
				Message: "Subjects",
				Help:    "Streams consume messages from subjects, this is a space or comma separated list that can include wildcards. Settable using --subjects",
			}, &subjects, survey.WithValidator(survey.Required))
			fisk.FatalIfError(err, "invalid input")

			c.subjects = iu.SplitString(subjects)
		}

		c.subjects = iu.SplitCLISubjects(c.subjects)
	}

	if c.mirror != "" && len(c.subjects) > 0 {
		fisk.Fatalf("mirrors cannot listen for messages on subjects")
	}

	if c.acceptDefaults {
		if c.storage == "" {
			c.storage = "file"
		}
		if c.compression == "" {
			c.compression = "none"
		}
		if c.replicas == 0 {
			c.replicas = 1
		}
		if c.retentionPolicyS == "" {
			c.retentionPolicyS = "Limits"
		}
		if c.discardPolicy == "" {
			c.discardPolicy = "Old"
		}
		if c.maxMsgLimit == 0 {
			c.maxMsgLimit = -1
		}
		if c.maxMsgPerSubjectLimit == 0 {
			c.maxMsgPerSubjectLimit = -1
		}
		if c.maxBytesLimitString == "" {
			c.maxBytesLimit = -1
		}
		if requireSize && c.maxBytesLimitString == "" {
			c.maxBytesLimit = 256 * 1024 * 1024
		}
		if c.maxAgeLimit == "" {
			c.maxAgeLimit = "-1"
		}
		if c.maxMsgSizeString == "" {
			c.maxMsgSize = -1
		}
	}

	if c.storage == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Storage",
			Options: []string{"file", "memory"},
			Help:    "Streams are stored on the server, this can be one of many backends and all are usable in clustering mode. Settable using --storage",
		}, &c.storage, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "invalid input")
	}

	storage := c.storeTypeFromString(c.storage)

	var compression api.Compression
	err = compression.UnmarshalJSON([]byte(fmt.Sprintf("%q", c.compression)))
	fisk.FatalIfError(err, "invalid compression algorithm")

	if c.replicas == 0 {
		c.replicas, err = askOneInt("Replication", "1", "When clustered, defines how many replicas of the data to store.  Settable using --replicas")
		fisk.FatalIfError(err, "invalid input")
	}
	if c.replicas <= 0 {
		fisk.Fatalf("replicas should be >= 1")
	}

	if c.retentionPolicyS == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Retention Policy",
			Options: []string{"Limits", "Interest", "Work Queue"},
			Help:    "Messages are retained either based on limits like size and age (Limits), as long as there are Consumers (Interest) or until any worker processed them (Work Queue)",
			Default: "Limits",
		}, &c.retentionPolicyS, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "invalid input")
	}

	if c.discardPolicy == "" {
		err = iu.AskOne(&survey.Select{
			Message: "Discard Policy",
			Options: []string{"New", "Old"},
			Help:    "Once the Stream reaches its limits of size or messages, the New policy will prevent further messages from being added while Old will delete old messages.",
			Default: "Old",
		}, &c.discardPolicy, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "invalid input")
	}

	if c.maxMsgLimit == 0 {
		c.maxMsgLimit, err = askOneInt("Stream Messages Limit", "-1", "Defines the amount of messages to keep in the store for this Stream, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs")
		fisk.FatalIfError(err, "invalid input")
		if c.maxMsgLimit <= 0 {
			c.maxMsgLimit = -1
		}
	}

	if c.maxMsgPerSubjectLimit == 0 && len(c.subjects) > 0 && (len(c.subjects) > 0 || strings.Contains(c.subjects[0], "*") || strings.Contains(c.subjects[0], ">")) {
		c.maxMsgPerSubjectLimit, err = askOneInt("Per Subject Messages Limit", "-1", "Defines the amount of messages to keep in the store for this Stream per unique subject, when exceeded oldest messages are removed, -1 for unlimited. Settable using --max-msgs-per-subject")
		fisk.FatalIfError(err, "invalid input")
		if c.maxMsgPerSubjectLimit <= 0 {
			c.maxMsgPerSubjectLimit = -1
		}
	}

	var maxAge time.Duration

	if c.maxBytesLimit == 0 {
		reqd := ""
		defltSize := "-1"
		if requireSize {
			reqd = "MaxBytes is required per Account Settings"
			defltSize = "256MB"
		}

		c.maxBytesLimit, err = askOneBytes("Total Stream Size", defltSize, "Defines the combined size of all messages in a Stream, when exceeded messages are removed or new ones are rejected, -1 for unlimited. Settable using --max-bytes", reqd)
		fisk.FatalIfError(err, "invalid input")
	}

	if c.maxBytesLimit <= 0 {
		c.maxBytesLimit = -1
	}

	if c.maxAgeLimit == "" {
		err = iu.AskOne(&survey.Input{
			Message: "Message TTL",
			Default: "-1",
			Help:    "Defines the oldest messages that can be stored in the Stream, any messages older than this period will be removed, -1 for unlimited. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Settable using --max-age",
		}, &c.maxAgeLimit)
		fisk.FatalIfError(err, "invalid input")
	}

	if c.maxAgeLimit != "-1" {
		maxAge, err = fisk.ParseDuration(c.maxAgeLimit)
		fisk.FatalIfError(err, "invalid maximum age limit format")
	}

	if c.maxMsgSize == 0 {
		c.maxMsgSize, err = askOneBytes("Max Message Size", "-1", "Defines the maximum size any single message may be to be accepted by the Stream. Settable using --max-msg-size", "")
		fisk.FatalIfError(err, "invalid input")
	}

	if c.maxMsgSize == 0 {
		c.maxMsgSize = -1
	}

	if c.maxMsgSize > math.MaxInt32 {
		fisk.Fatalf("max value size %s is too big maximum is %s", f(c.maxMsgSize), f(math.MaxInt32))
	}

	var dupeWindow time.Duration

	if c.dupeWindow == "" && c.mirror == "" {
		defaultDW := (2 * time.Minute).String()
		if maxAge > 0 && maxAge < 2*time.Minute {
			defaultDW = maxAge.String()
		}

		if c.acceptDefaults {
			c.dupeWindow = defaultDW
		} else {
			err = iu.AskOne(&survey.Input{
				Message: "Duplicate tracking time window",
				Default: defaultDW,
				Help:    "Duplicate messages are identified by the Msg-Id headers and tracked within a window of this size. Supports units (s)econds, (m)inutes, (h)ours, (y)ears, (M)onths, (d)ays. Settable using --dupe-window",
			}, &c.dupeWindow)
			fisk.FatalIfError(err, "invalid input")
		}
	}

	if c.dupeWindow != "" {
		dupeWindow, err = fisk.ParseDuration(c.dupeWindow)
		fisk.FatalIfError(err, "invalid duplicate window format")
	}

	if !c.acceptDefaults {
		if !c.allowRollupSet {
			c.allowRollup, err = askConfirmation("Allow message Roll-ups", false)
			fisk.FatalIfError(err, "invalid input")
		}

		if !c.denyDeleteSet {
			allow, err := askConfirmation("Allow message deletion", true)
			fisk.FatalIfError(err, "invalid input")
			c.denyDelete = !allow
		}

		if !c.denyPurgeSet {
			allow, err := askConfirmation("Allow purging subjects or the entire stream", true)
			fisk.FatalIfError(err, "invalid input")
			c.denyPurge = !allow
		}
	}

	cfg := api.StreamConfig{
		Name:                   c.stream,
		Description:            c.description,
		Subjects:               c.subjects,
		MaxMsgs:                c.maxMsgLimit,
		MaxMsgsPer:             c.maxMsgPerSubjectLimit,
		MaxBytes:               c.maxBytesLimit,
		MaxMsgSize:             int32(c.maxMsgSize),
		Duplicates:             dupeWindow,
		MaxAge:                 maxAge,
		Storage:                storage,
		Compression:            compression,
		FirstSeq:               c.firstSeq,
		NoAck:                  !c.ack,
		Retention:              c.retentionPolicyFromString(),
		Discard:                c.discardPolicyFromString(),
		MaxConsumers:           c.maxConsumers,
		Replicas:               int(c.replicas),
		RollupAllowed:          c.allowRollup,
		DenyPurge:              c.denyPurge,
		DenyDelete:             c.denyDelete,
		AllowDirect:            c.allowDirect,
		AllowMsgTTL:            c.allowMsgTTL,
		SubjectDeleteMarkerTTL: c.subjectDeleteMarkerTTL,
		MirrorDirect:           c.allowMirrorDirectSet,
		DiscardNewPer:          c.discardPerSubj,
	}

	if c.limitInactiveThreshold > 0 {
		cfg.ConsumerLimits.InactiveThreshold = c.limitInactiveThreshold
	}

	if c.limitMaxAckPending > 0 {
		cfg.ConsumerLimits.MaxAckPending = c.limitMaxAckPending
	}

	if len(c.metadata) > 0 {
		cfg.Metadata = c.metadata
	}

	if c.placementCluster != "" || len(c.placementTags) > 0 {
		cfg.Placement = &api.Placement{
			Cluster: c.placementCluster,
			Tags:    c.placementTags,
		}
	}

	if c.mirror != "" {
		if iu.IsJsonObjectString(c.mirror) {
			cfg.Mirror, err = c.parseStreamSource(c.mirror)
			fisk.FatalIfError(err, "invalid mirror")
		} else {
			if !c.acceptDefaults {
				cfg.Mirror = c.askMirror()
			}
		}
	}

	for _, source := range c.sources {
		if iu.IsJsonObjectString(source) {
			ss, err := c.parseStreamSource(source)
			fisk.FatalIfError(err, "invalid source")
			cfg.Sources = append(cfg.Sources, ss)
		} else {
			ss := c.askSource(source, fmt.Sprintf("%s Source", source))
			cfg.Sources = append(cfg.Sources, ss)
		}
	}

	c.checkRepubTransform()

	if c.repubSource != "" && c.repubDest != "" {
		cfg.RePublish = &api.RePublish{
			Source:      c.repubSource,
			Destination: c.repubDest,
			HeadersOnly: c.repubHeadersOnly,
		}
	}

	if c.subjectTransformSource != "" && c.subjectTransformDest != "" {
		cfg.SubjectTransform = &api.SubjectTransformConfig{
			Source:      c.subjectTransformSource,
			Destination: c.subjectTransformDest,
		}
	}

	cfg.Metadata = iu.RemoveReservedMetadata(cfg.Metadata)

	return cfg
}

func (c *streamCmd) askMirror() *api.StreamSource {
	mirror := &api.StreamSource{Name: c.mirror}
	ok, err := askConfirmation("Adjust mirror start", false)
	fisk.FatalIfError(err, "Could not request mirror details")
	if ok {
		a, err := askOneInt("Mirror Start Sequence", "0", "Start mirroring at a specific sequence")
		fisk.FatalIfError(err, "Invalid sequence")
		mirror.OptStartSeq = uint64(a)

		if mirror.OptStartSeq == 0 {
			ts := ""
			err = iu.AskOne(&survey.Input{
				Message: "Mirror Start Time (YYYY:MM:DD HH:MM:SS)",
				Help:    "Start replicating as a specific time stamp in UTC time",
			}, &ts)
			fisk.FatalIfError(err, "could not request start time")
			if ts != "" {
				t, err := time.Parse("2006:01:02 15:04:05", ts)
				fisk.FatalIfError(err, "invalid time format")
				mirror.OptStartTime = &t
			}
		}
	}

	ok, err = askConfirmation("Adjust mirror filter and transform", false)
	fisk.FatalIfError(err, "Could not request mirror details")

	if ok {
		var sources []string
		var destinations []string

		for {
			var source string
			var destination string

			err = iu.AskOne(&survey.Input{
				Message: "Filter mirror by subject (hit enter to finish)",
				Help:    "Only replicate data matching this subject",
			}, &source)
			fisk.FatalIfError(err, "could not request filter")

			if source == "" {
				break
			}

			err = iu.AskOne(&survey.Input{
				Message: "Subject transform destination",
				Help:    "Transform the subjects using this destination (hit enter for no transformation)",
			}, &destination)
			fisk.FatalIfError(err, "could not request transform destination")

			sources = append(sources, source)
			destinations = append(destinations, destination)
		}

		for i := range sources {
			mirror.SubjectTransforms = append(mirror.SubjectTransforms, api.SubjectTransformConfig{
				Source:      sources[i],
				Destination: destinations[i],
			})
		}
	}

	ok, err = askConfirmation("Import mirror from a different JetStream domain", false)
	fisk.FatalIfError(err, "Could not request mirror details")
	if ok {
		mirror.External = &api.ExternalStream{}
		domainName := ""
		err = iu.AskOne(&survey.Input{
			Message: "Foreign JetStream domain name",
			Help:    "The domain name from where to import the JetStream API",
		}, &domainName, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "Could not request mirror details")
		mirror.External.ApiPrefix = fmt.Sprintf("$JS.%s.API", domainName)

		err = iu.AskOne(&survey.Input{
			Message: "Delivery prefix",
			Help:    "Optional prefix of the delivery subject",
		}, &mirror.External.DeliverPrefix)
		fisk.FatalIfError(err, "Could not request mirror details")
		return mirror
	}

	ok, err = askConfirmation("Import mirror from a different account", false)
	fisk.FatalIfError(err, "Could not request mirror details")
	if !ok {
		return mirror
	}

	mirror.External = &api.ExternalStream{}
	err = iu.AskOne(&survey.Input{
		Message: "Foreign account API prefix",
		Help:    "The prefix where the foreign account JetStream API has been imported",
	}, &mirror.External.ApiPrefix, survey.WithValidator(survey.Required))
	fisk.FatalIfError(err, "Could not request mirror details")

	err = iu.AskOne(&survey.Input{
		Message: "Foreign account delivery prefix",
		Help:    "The prefix where the foreign account JetStream delivery subjects has been imported",
	}, &mirror.External.DeliverPrefix, survey.WithValidator(survey.Required))
	fisk.FatalIfError(err, "Could not request mirror details")

	return mirror
}

func (c *streamCmd) askSource(name string, prefix string) *api.StreamSource {
	cfg := &api.StreamSource{Name: name}

	ok, err := askConfirmation(fmt.Sprintf("Adjust source %q start", name), false)
	fisk.FatalIfError(err, "Could not request source details")
	if ok {
		a, err := askOneInt(fmt.Sprintf("%s Start Sequence", prefix), "0", "Start mirroring at a specific sequence")
		fisk.FatalIfError(err, "Invalid sequence")
		cfg.OptStartSeq = uint64(a)

		ts := ""
		err = iu.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s UTC Time Stamp (YYYY:MM:DD HH:MM:SS)", prefix),
			Help:    "Start replicating as a specific time stamp",
		}, &ts)
		fisk.FatalIfError(err, "could not request start time")
		if ts != "" {
			t, err := time.Parse("2006:01:02 15:04:05", ts)
			fisk.FatalIfError(err, "invalid time format")
			cfg.OptStartTime = &t
		}
	}

	ok, err = askConfirmation(fmt.Sprintf("Adjust source %q filter and transform", name), false)
	fisk.FatalIfError(err, "Could not request source details")

	if ok {
		var sources []string
		var destinations []string
		for {
			var source string
			var destination string

			err = iu.AskOne(&survey.Input{
				Message: "Filter source by subject (hit enter to finish)",
				Help:    "Only replicate data matching this subject",
			}, &source)
			fisk.FatalIfError(err, "could not request filter")

			if source == "" {
				break
			}

			err = iu.AskOne(&survey.Input{
				Message: "Subject transform destination",
				Help:    "Transform the subjects using this destination (hit enter for no transformation)",
			}, &destination)
			fisk.FatalIfError(err, "could not request transform destination")

			sources = append(sources, source)
			destinations = append(destinations, destination)
		}

		for i := range sources {
			cfg.SubjectTransforms = append(cfg.SubjectTransforms, api.SubjectTransformConfig{
				Source:      sources[i],
				Destination: destinations[i],
			})
		}
	}

	ok, err = askConfirmation(fmt.Sprintf("Import %q from a different JetStream domain", name), false)
	fisk.FatalIfError(err, "Could not request source details")
	if ok {
		cfg.External = &api.ExternalStream{}
		domainName := ""
		err = iu.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s foreign JetStream domain name", prefix),
			Help:    "The domain name from where to import the JetStream API",
		}, &domainName, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "Could not request source details")
		cfg.External.ApiPrefix = fmt.Sprintf("$JS.%s.API", domainName)

		err = iu.AskOne(&survey.Input{
			Message: fmt.Sprintf("%s foreign JetStream domain delivery prefix", prefix),
			Help:    "Optional prefix of the delivery subject",
		}, &cfg.External.DeliverPrefix)
		fisk.FatalIfError(err, "Could not request source details")
		return cfg
	}

	ok, err = askConfirmation(fmt.Sprintf("Import %q from a different account", name), false)
	fisk.FatalIfError(err, "Could not request source details")
	if !ok {
		return cfg
	}

	cfg.External = &api.ExternalStream{}
	err = iu.AskOne(&survey.Input{
		Message: fmt.Sprintf("%s foreign account API prefix", prefix),
		Help:    "The prefix where the foreign account JetStream API has been imported",
	}, &cfg.External.ApiPrefix, survey.WithValidator(survey.Required))
	fisk.FatalIfError(err, "Could not request source details")

	err = iu.AskOne(&survey.Input{
		Message: fmt.Sprintf("%s foreign account delivery prefix", prefix),
		Help:    "The prefix where the foreign account JetStream delivery subjects has been imported",
	}, &cfg.External.DeliverPrefix, survey.WithValidator(survey.Required))
	fisk.FatalIfError(err, "Could not request source details")

	return cfg
}

func (c *streamCmd) parseStreamSource(source string) (*api.StreamSource, error) {
	ss := &api.StreamSource{}

	if iu.IsJsonObjectString(source) {
		err := json.Unmarshal([]byte(source), ss)
		if err != nil {
			return nil, err
		}

		if ss.Name == "" {
			return nil, fmt.Errorf("name is required")
		}
	} else {
		ss.Name = source
	}

	return ss, nil
}

func (c *streamCmd) validateCfg(cfg *api.StreamConfig) (bool, []byte, []string, error) {
	if os.Getenv("NOVALIDATE") != "" {
		return true, nil, nil, nil
	}

	j, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return false, nil, nil, err
	}

	if !cfg.NoAck {
		for _, subject := range cfg.Subjects {
			if subject == ">" {
				return false, j, []string{"subjects cannot be '>' when acknowledgement is enabled"}, nil
			}
		}
	}

	valid, errs := cfg.Validate(new(SchemaValidator))

	return valid, j, errs, nil
}

func (c *streamCmd) addAction(pc *fisk.ParseContext) (err error) {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "could not create Stream")

	requireSize, _ := mgr.IsStreamMaxBytesRequired()

	cfg := c.prepareConfig(pc, requireSize)

	switch {
	case c.validateOnly:
		valid, j, errs, err := c.validateCfg(&cfg)
		if err != nil {
			return err
		}

		fmt.Println(string(j))
		fmt.Println()
		if !valid {
			fisk.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		fmt.Printf("Configuration is a valid Stream matching %s\n", cfg.SchemaType())
		return nil

	case c.outFile != "":
		valid, j, errs, err := c.validateCfg(&cfg)
		fisk.FatalIfError(err, "Could not validate configuration")

		if !valid {
			fisk.Fatalf("Validation Failed: %s", strings.Join(errs, "\n\t"))
		}

		return os.WriteFile(c.outFile, j, 0600)
	}

	str, err := mgr.NewStreamFromDefault(c.stream, cfg)
	fisk.FatalIfError(err, "could not create Stream")

	fmt.Printf("Stream %s was created\n\n", c.stream)

	c.showStream(str)

	return nil
}

func (c *streamCmd) rmAction(_ *fisk.ParseContext) (err error) {
	if c.force {
		if c.stream == "" {
			return fmt.Errorf("--force requires a stream name")
		}

		c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
		fisk.FatalIfError(err, "setup failed")

		err = c.mgr.DeleteStream(c.stream)
		if err != nil {
			if err == context.DeadlineExceeded {
				fmt.Println("Delete failed due to timeout, the stream might not exist or be in an unmanageable state")
			}
		}

		return err
	}

	c.connectAndAskStream()

	ok, err := askConfirmation(fmt.Sprintf("Really delete Stream %s", c.stream), false)
	fisk.FatalIfError(err, "could not obtain confirmation")

	if !ok {
		return nil
	}

	stream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not remove Stream")

	err = stream.Delete()
	fisk.FatalIfError(err, "could not remove Stream")

	return nil
}

func (c *streamCmd) purgeAction(_ *fisk.ParseContext) (err error) {
	c.connectAndAskStream()

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really purge Stream %s", c.stream), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	if jsm.IsKVBucketStream(c.stream) {
		err := c.kvAbstractionWarn(c.stream, "Really operate on the KV stream?")
		if err != nil {
			return err
		}
	}

	stream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not purge Stream")

	var req *api.JSApiStreamPurgeRequest
	if c.purgeKeep > 0 || c.purgeSubject != "" || c.purgeSequence > 0 {
		if c.purgeSequence > 0 && c.purgeKeep > 0 {
			return fmt.Errorf("sequence and keep cannot be combined when purging")
		}

		req = &api.JSApiStreamPurgeRequest{
			Sequence: c.purgeSequence,
			Subject:  c.purgeSubject,
			Keep:     c.purgeKeep,
		}
	}

	err = stream.Purge(req)
	fisk.FatalIfError(err, "could not purge Stream")

	stream.Reset()

	c.showStream(stream)

	return nil
}

func (c *streamCmd) lsNames(mgr *jsm.Manager, filter *jsm.StreamNamesFilter) error {
	names, err := mgr.StreamNames(filter)
	if err != nil {
		return err
	}

	if c.json {
		err = iu.PrintJSON(names)
		fisk.FatalIfError(err, "could not display Streams")
		return nil
	}

	for _, n := range names {
		fmt.Println(n)
	}

	return nil
}

func (c *streamCmd) lsAction(_ *fisk.ParseContext) error {
	_, mgr, err := prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")

	var filter *jsm.StreamNamesFilter
	if c.filterSubject != "" {
		filter = &jsm.StreamNamesFilter{Subject: c.filterSubject}
	}

	if c.listNames {
		return c.lsNames(mgr, filter)
	}

	var streams []*jsm.Stream
	var names []string

	skipped := false

	missing, err := mgr.EachStream(filter, func(s *jsm.Stream) {
		if !c.showAll && s.IsInternal() {
			skipped = true
			return
		}

		streams = append(streams, s)
		names = append(names, s.Name())
	})
	if err != nil {
		return fmt.Errorf("could not list streams: %s", err)
	}

	if c.json {
		err = iu.PrintJSON(names)
		fisk.FatalIfError(err, "could not display Streams")
		return nil
	}

	if len(streams) == 0 && skipped {
		fmt.Println("No Streams defined, pass -a to include system streams")
		return nil
	} else if len(streams) == 0 {
		fmt.Println("No Streams defined")
		return nil
	}

	out, err := c.renderStreamsAsTable(streams, missing)
	if err != nil {
		return err
	}

	fmt.Println(out)

	return nil
}

func (c *streamCmd) renderStreamsAsList(streams []*jsm.Stream, missing []string) string {
	var names []string
	for _, s := range streams {
		names = append(names, s.Name())
	}
	names = append(names, missing...)

	sort.Strings(names)

	return strings.Join(names, "\n")
}

func (c *streamCmd) renderStreamsAsTable(streams []*jsm.Stream, missing []string) (string, error) {
	sort.Slice(streams, func(i, j int) bool {
		info, _ := streams[i].LatestInformation()
		jnfo, _ := streams[j].LatestInformation()

		return info.State.Bytes < jnfo.State.Bytes
	})

	var out bytes.Buffer
	var table *iu.Table
	if c.filterSubject == "" {
		table = iu.NewTableWriter(opts(), "Streams")
	} else {
		table = iu.NewTableWriter(opts(), fmt.Sprintf("Streams matching %s", c.filterSubject))
	}

	table.AddHeaders("Name", "Description", "Created", "Messages", "Size", "Last Message")
	for _, s := range streams {
		nfo, _ := s.LatestInformation()
		table.AddRow(s.Name(), s.Description(), f(nfo.Created.Local()), f(nfo.State.Msgs), humanize.IBytes(nfo.State.Bytes), f(sinceRefOrNow(nfo.TimeStamp, nfo.State.LastTime)))
	}

	fmt.Fprintln(&out, table.Render())

	c.renderMissing(&out, missing)

	return out.String(), nil
}

func (c *streamCmd) renderMissing(out io.Writer, missing []string) {
	toany := func(items []string) (res []any) {
		for _, i := range items {
			res = append(res, any(i))
		}
		return res
	}

	if len(missing) > 0 {
		fmt.Fprintln(out)
		sort.Strings(missing)
		table := iu.NewTableWriter(opts(), "Inaccessible Streams")
		iu.SliceGroups(missing, 4, func(names []string) {
			table.AddRow(toany(names)...)
		})
		fmt.Fprint(out, table.Render())
	}
}

func (c *streamCmd) rmMsgAction(_ *fisk.ParseContext) (err error) {
	c.connectAndAskStream()

	if c.msgID == -1 {
		id := ""
		err = iu.AskOne(&survey.Input{
			Message: "Message Sequence to remove",
		}, &id, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		fisk.FatalIfError(err, "invalid number")

		if idint <= 0 {
			return fmt.Errorf("positive message ID required")
		}
		c.msgID = int64(idint)
	}

	stream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not load Stream %s", c.stream)

	if jsm.IsKVBucketStream(c.stream) {
		err := c.kvAbstractionWarn(c.stream, "Really operate on the KV stream?")
		if err != nil {
			return err
		}
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove message %d from Stream %s", c.msgID, c.stream), false)
		fisk.FatalIfError(err, "could not obtain confirmation")

		if !ok {
			return nil
		}
	}

	return stream.DeleteMessageRequest(api.JSApiMsgDeleteRequest{Seq: uint64(c.msgID)})
}

func (c *streamCmd) getAction(_ *fisk.ParseContext) (err error) {
	c.connectAndAskStream()

	if c.msgID == -1 && c.filterSubject == "" {
		id := ""
		err = iu.AskOne(&survey.Input{
			Message: "Message Sequence to retrieve",
			Default: "-1",
		}, &id, survey.WithValidator(survey.Required))
		fisk.FatalIfError(err, "invalid input")

		idint, err := strconv.Atoi(id)
		fisk.FatalIfError(err, "invalid number")

		c.msgID = int64(idint)

		if c.msgID == -1 {
			err = iu.AskOne(&survey.Input{
				Message: "Subject to retrieve last message for",
			}, &c.filterSubject)
			fisk.FatalIfError(err, "invalid subject")
		}
	}

	stream, err := c.loadStream(c.stream)
	fisk.FatalIfError(err, "could not load Stream %s", c.stream)

	var item *api.StoredMsg
	if c.msgID > -1 {
		item, err = stream.ReadMessage(uint64(c.msgID))
	} else if c.filterSubject != "" {
		item, err = stream.ReadLastMessageForSubject(c.filterSubject)
	} else {
		return fmt.Errorf("no ID or subject specified")
	}
	fisk.FatalIfError(err, "could not retrieve %s#%d", c.stream, c.msgID)

	if c.json {
		iu.PrintJSON(item)
		return nil
	}

	fmt.Printf("Item: %s#%d received %v (%s) on Subject %s\n\n", c.stream, item.Sequence, item.Time, f(time.Since(item.Time)), item.Subject)

	if len(item.Header) > 0 {
		fmt.Println("Headers:")
		hdrs, err := iu.DecodeHeadersMsg(item.Header)
		if err == nil {
			for k, vals := range hdrs {
				for _, val := range vals {
					fmt.Printf("  %s: %s\n", k, val)
				}
			}
		}
		fmt.Println()
	}
	outPutMSGBody(item.Data, c.vwTranslate, item.Subject, c.stream)
	return nil
}

func (c *streamCmd) connectAndAskStream() bool {
	var err error

	shouldAsk := c.stream == ""
	c.nc, c.mgr, err = prepareHelper("", natsOpts()...)
	fisk.FatalIfError(err, "setup failed")

	c.stream, c.selectedStream, err = selectStream(c.mgr, c.stream, c.force, c.showAll)
	fisk.FatalIfError(err, "could not pick a Stream to operate on")

	return shouldAsk
}

func (c *streamCmd) boolReverse(v bool) bool {
	if c.reportSortReverse {
		return !v
	}

	return v
}
