// Copyright 2023-2025 The NATS Authors
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
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"time"

	au "github.com/nats-io/natscli/internal/auth"
	iu "github.com/nats-io/natscli/internal/util"
	"gopkg.in/yaml.v3"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/nats-io/nats-server/v2/server"
	ab "github.com/synadia-io/jwt-auth-builder.go"
)

type authAccountCommand struct {
	accountName             string
	advertise               bool
	advertiseIsSet          bool
	bearerAllowed           bool
	bearerAllowedIsSet      bool
	connTypes               []string
	defaults                bool
	description             string
	descriptionIsSet        bool
	expiry                  time.Duration
	exportName              string
	force                   bool
	isService               bool
	jetStream               bool
	jetStreamIsSet          bool
	listNames               bool
	locale                  string
	maxAckPending           int64
	maxAckPendingIsSet      bool
	maxConns                int64
	maxConnsIsSet           bool
	maxConsumers            int64
	maxConsumersIsSet       bool
	maxExports              int64
	maxExportsIsSet         bool
	maxImports              int64
	maxImportsIsSet         bool
	maxLeafnodes            int64
	maxLeafNodesIsSet       bool
	maxPayload              int64
	maxPayloadString        string
	maxStreams              int64
	maxStreamsIsSet         bool
	maxSubs                 int64
	maxSubIsSet             bool
	memMax                  int64
	memMaxStream            int64
	memMaxStreamString      string
	memMaxString            string
	operatorName            string
	output                  string
	pubAllow                []string
	pubDeny                 []string
	showJWT                 bool
	skRole                  string
	storeMax                int64
	storeMaxStream          int64
	storeMaxStreamString    string
	storeMaxString          string
	streamSizeRequired      bool
	streamSizeRequiredIsSet bool
	subAllow                []string
	subDeny                 []string
	subject                 string
	tokenPosition           uint
	url                     *url.URL
	importName              string
	localSubject            string
	share                   bool
	shareIsSet              bool
	allowTrace              bool
	allowTraceIsSet         bool
	importAccount           string
	bucketName              string
	prefix                  string
	tags                    []string
	rmTags                  []string
	signingKey              string
	mapSource               string
	mapTarget               string
	mapWeight               uint
	mapCluster              string
	inputFile               string
	clusterTraffic          string
	clusterTrafficIsSet     bool
}

func configureAuthAccountCommand(auth commandHost) {
	c := &authAccountCommand{}

	acct := auth.Command("account", "Manage NATS Accounts").Alias("a").Alias("acct").Alias("act")

	addCreateFlags := func(f *fisk.CmdClause, edit bool) {
		f.Flag("bearer", "Allows bearer tokens").Default("false").IsSetByUser(&c.bearerAllowedIsSet).BoolVar(&c.bearerAllowed)
		f.Flag("connections", "Maximum allowed connections").Default("-1").IsSetByUser(&c.maxConnsIsSet).Int64Var(&c.maxConns)
		f.Flag("expiry", "How long this account should be valid for as a duration").PlaceHolder("DURATION").DurationVar(&c.expiry)
		f.Flag("exports", "Maximum allowed exports").Default("-1").IsSetByUser(&c.maxExportsIsSet).Int64Var(&c.maxExports)
		f.Flag("imports", "Maximum allowed imports").Default("-1").IsSetByUser(&c.maxImportsIsSet).Int64Var(&c.maxImports)
		f.Flag("jetstream", "Enables JetStream").Default("false").IsSetByUser(&c.jetStreamIsSet).BoolVar(&c.jetStream)
		f.Flag("js-consumers", "Sets the maximum Consumers the account can have").Default("-1").IsSetByUser(&c.maxConsumersIsSet).Int64Var(&c.maxConsumers)
		f.Flag("js-disk", "Sets a Disk Storage quota").PlaceHolder("BYTES").StringVar(&c.storeMaxString)
		f.Flag("js-disk-stream", "Sets the maximum size a Disk Storage stream may be").PlaceHolder("BYTES").Default("-1").StringVar(&c.storeMaxStreamString)
		f.Flag("js-max-pending", "Default Max Ack Pending for Tier 0 limits").PlaceHolder("MESSAGES").IsSetByUser(&c.maxAckPendingIsSet).Int64Var(&c.maxAckPending)
		f.Flag("js-memory", "Sets a Memory Storage quota").PlaceHolder("BYTES").StringVar(&c.memMaxString)
		f.Flag("js-memory-stream", "Sets the maximum size a Memory Storage stream may be").PlaceHolder("BYTES").Default("-1").StringVar(&c.memMaxStreamString)
		f.Flag("js-stream-size-required", "Requires Streams to have a maximum size declared").IsSetByUser(&c.streamSizeRequiredIsSet).UnNegatableBoolVar(&c.streamSizeRequired)
		f.Flag("js-streams", "Sets the maximum Streams the account can have").Default("-1").IsSetByUser(&c.maxStreamsIsSet).Int64Var(&c.maxStreams)
		f.Flag("js-cluster-traffic", "Sets the account used for JetStream cluster traffic").PlaceHolder("ACCOUNT").IsSetByUser(&c.clusterTrafficIsSet).EnumVar(&c.clusterTraffic, "owner", "system")
		f.Flag("leafnodes", "Maximum allowed Leafnode connections").Default("-1").IsSetByUser(&c.maxLeafNodesIsSet).Int64Var(&c.maxLeafnodes)
		f.Flag("payload", "Maximum allowed payload").PlaceHolder("BYTES").Default("-1").StringVar(&c.maxPayloadString)
		f.Flag("subscriptions", "Maximum allowed subscriptions").Default("-1").IsSetByUser(&c.maxSubIsSet).Int64Var(&c.maxSubs)
		f.Flag("tags", "Tags to assign to this Account").StringsVar(&c.tags)
		if edit {
			f.Flag("no-tags", "Tags to remove from this Account").StringsVar(&c.rmTags)
		}
	}

	add := acct.Command("add", "Adds a new Account").Alias("create").Alias("new").Action(c.addAction)
	add.Arg("account", "Unique name for this Account").StringVar(&c.accountName)
	add.Flag("operator", "Operator to add the account to").StringVar(&c.operatorName)
	add.Flag("key", "The public key to use when signing the user").StringVar(&c.signingKey)
	addCreateFlags(add, false)
	add.Flag("defaults", "Accept default values without prompting").UnNegatableBoolVar(&c.defaults)

	info := acct.Command("info", "Show Account information").Alias("i").Alias("show").Alias("view").Action(c.infoAction)
	info.Arg("account", "Account to view").StringVar(&c.accountName)
	info.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)

	edit := acct.Command("edit", "Edit Account settings").Alias("update").Action(c.editAction)
	edit.Arg("account", "Unique name for this Account").StringVar(&c.accountName)
	edit.Flag("operator", "Operator to add the account to").StringVar(&c.operatorName)
	addCreateFlags(edit, false)

	ls := acct.Command("ls", "List Accounts").Action(c.lsAction)
	ls.Arg("operator", "Operator to act on").StringVar(&c.operatorName)
	ls.Flag("names", "Show just the Account names").UnNegatableBoolVar(&c.listNames)

	//rm := acct.Command("rm", "Removes an Account").Action(c.rmAction)
	//rm.Arg("name", "Account to view").StringVar(&c.accountName)
	//rm.Flag("operator", "Operator hosting the Account").StringVar(&c.operatorName)
	//rm.Flag("force", "Removes without prompting").Short('f').UnNegatableBoolVar(&c.force)

	push := acct.Command("push", "Push the Account to the NATS Resolver").Action(c.pushAction)
	push.Arg("account", "Account to act on").StringVar(&c.accountName)
	push.Flag("operator", "Operator to act on").StringVar(&c.operatorName)
	push.Flag("show", "Show the Account JWT before pushing").UnNegatableBoolVar(&c.showJWT)

	query := acct.Command("query", "Pull the Account from the NATS Resolver and view it").Alias("pull").Action(c.queryAction)
	query.Arg("account", "Account to act on").Required().StringVar(&c.accountName)
	query.Arg("output", "Saves the JWT to a file").StringVar(&c.output)
	query.Flag("operator", "Operator to act on").StringVar(&c.operatorName)

	imports := acct.Command("imports", "Manage account Imports").Alias("i").Alias("imp").Alias("import")

	impAdd := imports.Command("add", "Adds an Import").Alias("new").Alias("a").Alias("n").Action(c.importAddAction)
	impAdd.Arg("name", "A unique name for the import").Required().StringVar(&c.importName)
	impAdd.Arg("subject", "The Subject to import").Required().StringVar(&c.subject)
	impAdd.Arg("account", "Account to import into").StringVar(&c.accountName)
	impAdd.Flag("source", "The account public key to import from").StringVar(&c.importAccount)
	impAdd.Flag("local", "The local Subject to use for the import").StringVar(&c.localSubject)
	impAdd.Flag("share", "Shares connection information with the exporter").UnNegatableBoolVar(&c.share)
	impAdd.Flag("traceable", "Enable tracing messages across Stream imports").UnNegatableBoolVar(&c.allowTrace)
	impAdd.Flag("service", "Sets the import to be a Service rather than a Stream").UnNegatableBoolVar(&c.isService)
	impAdd.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)

	impInfo := imports.Command("info", "Show information for an Import").Alias("i").Alias("show").Alias("view").Action(c.importInfoAction)
	impInfo.Arg("subject", "Export to view by subject").StringVar(&c.subject)
	impInfo.Arg("account", "Account to act on").StringVar(&c.accountName)
	impInfo.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)

	impEdit := imports.Command("edit", "Edits an Import").Alias("update").Action(c.importEditAction)
	impEdit.Arg("subject", "The Local import Subject to edit").Required().StringVar(&c.subject)
	impEdit.Arg("account", "Account to act on").StringVar(&c.accountName)
	impEdit.Flag("local", "The local Subject to use for the import").StringVar(&c.localSubject)
	impEdit.Flag("share", "Shares connection information with the exporter").IsSetByUser(&c.shareIsSet).UnNegatableBoolVar(&c.share)
	impEdit.Flag("traceable", "Enable tracing messages across Stream imports").IsSetByUser(&c.allowTraceIsSet).UnNegatableBoolVar(&c.allowTrace)
	impEdit.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)

	impLs := imports.Command("ls", "List Imports").Alias("list").Action(c.importLsAction)
	impLs.Arg("account", "Account to act on").StringVar(&c.accountName)
	impLs.Arg("operator", "Operator to act on").StringVar(&c.operatorName)

	impRm := imports.Command("rm", "Removes an Import").Action(c.importRmAction)
	impRm.Arg("subject", "Import to remove by local subject").Required().StringVar(&c.subject)
	impRm.Arg("account", "Account to act on").StringVar(&c.accountName)
	impRm.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)
	impRm.Flag("force", "Removes without prompting").Short('f').UnNegatableBoolVar(&c.force)

	impKv := imports.Command("kv", "Imports a KV bucket").Hidden().Action(c.importKvAction)
	impKv.Arg("bucket", "The bucket to export").Required().StringVar(&c.bucketName)
	impKv.Arg("prefix", "The prefix to mount the bucket on").Required().StringVar(&c.prefix)
	impKv.Arg("source", "The account public key to import from").Required().StringVar(&c.importAccount)

	exports := acct.Command("exports", "Manage account Exports").Alias("e").Alias("exp").Alias("export")

	expAdd := exports.Command("add", "Adds an Export").Alias("new").Alias("a").Alias("n").Action(c.exportAddAction)
	expAdd.Arg("name", "A unique name for the Export").Required().StringVar(&c.exportName)
	expAdd.Arg("subject", "The Subject to export").Required().StringVar(&c.subject)
	expAdd.Arg("account", "Account to act on").StringVar(&c.accountName)
	expAdd.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)
	expAdd.Flag("description", "Friendly description").StringVar(&c.description)
	expAdd.Flag("url", "Sets a URL for further information").URLVar(&c.url)
	expAdd.Flag("token-position", "The position to use for the Account name").UintVar(&c.tokenPosition)
	expAdd.Flag("advertise", "Advertise the Export").UnNegatableBoolVar(&c.advertise)
	expAdd.Flag("service", "Sets the Export to be a Service rather than a Stream").UnNegatableBoolVar(&c.isService)

	expInfo := exports.Command("info", "Show information for an Export").Alias("i").Alias("show").Alias("view").Action(c.exportInfoAction)
	expInfo.Arg("subject", "Export to view by subject").StringVar(&c.subject)
	expInfo.Arg("account", "Account to act on").StringVar(&c.accountName)
	expInfo.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)

	expEdit := exports.Command("edit", "Edits an Export").Alias("update").Action(c.exportEditAction)
	expEdit.Arg("subject", "The Export Subject to edit").Required().StringVar(&c.subject)
	expEdit.Arg("account", "Account to act on").StringVar(&c.accountName)
	expEdit.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)
	expEdit.Flag("description", "Friendly description").IsSetByUser(&c.descriptionIsSet).StringVar(&c.description)
	expEdit.Flag("url", "Sets a URL for further information").URLVar(&c.url)
	expEdit.Flag("token-position", "The position to use for the Account name").UintVar(&c.tokenPosition)
	expEdit.Flag("advertise", "Advertise the Export").IsSetByUser(&c.advertiseIsSet).BoolVar(&c.advertise)

	expLs := exports.Command("ls", "List Exports").Alias("list").Action(c.exportLsAction)
	expLs.Arg("account", "Account to act on").StringVar(&c.accountName)
	expLs.Flag("operator", "Operator to act on").StringVar(&c.operatorName)

	expRm := exports.Command("rm", "Removes an Export").Action(c.exportRmAction)
	expRm.Arg("subject", "Export to remove by subject").Required().StringVar(&c.subject)
	expRm.Arg("account", "Account to act on").StringVar(&c.accountName)
	expRm.Flag("operator", "Operator hosting the account").StringVar(&c.operatorName)
	expRm.Flag("force", "Removes without prompting").Short('f').UnNegatableBoolVar(&c.force)

	expKv := exports.Command("kv", "Exports a KV bucket").Hidden().Action(c.exportKvAction)
	expKv.Arg("bucket", "The bucket to export").Required().StringVar(&c.bucketName)

	sk := acct.Command("keys", "Manage Scoped Signing Keys").Alias("sk").Alias("s")

	skadd := sk.Command("add", "Adds a signing key").Alias("new").Alias("a").Alias("n").Action(c.skAddAction)
	skadd.Arg("account", "Account to act on").StringVar(&c.accountName)
	skadd.Arg("role", "The role to add a key for").StringVar(&c.skRole)
	skadd.Flag("operator", "Operator to act on").StringVar(&c.operatorName)
	skadd.Flag("description", "Description for the signing key").StringVar(&c.description)
	skadd.Flag("subscriptions", "Maximum allowed subscriptions").Default("-1").Int64Var(&c.maxSubs)
	skadd.Flag("payload", "Maximum allowed payload").PlaceHolder("BYTES").StringVar(&c.maxPayloadString)
	skadd.Flag("bearer", "Allows bearer tokens").Default("false").BoolVar(&c.bearerAllowed)
	skadd.Flag("locale", "Locale for the client").StringVar(&c.locale)
	skadd.Flag("connection", "Set the allowed connections (nats, ws, wsleaf, mqtt)").EnumsVar(&c.connTypes, "nats", "ws", "leaf", "wsleaf", "mqtt")
	skadd.Flag("pub-allow", "Sets subjects where publishing is allowed").StringsVar(&c.pubAllow)
	skadd.Flag("pub-deny", "Sets subjects where publishing is allowed").StringsVar(&c.pubDeny)
	skadd.Flag("sub-allow", "Sets subjects where subscribing is allowed").StringsVar(&c.subAllow)
	skadd.Flag("sub-deny", "Sets subjects where subscribing is allowed").StringsVar(&c.subDeny)

	skInfo := sk.Command("info", "Show information for a Scoped Signing Key").Alias("i").Alias("show").Alias("view").Action(c.skInfoAction)
	skInfo.Arg("account", "Account to view").StringVar(&c.accountName)
	skInfo.Arg("key", "The role or key to view").StringVar(&c.skRole)
	skInfo.Flag("operator", "Operator to act on").StringVar(&c.operatorName)

	skls := sk.Command("ls", "List Scoped Signing Keys").Alias("list").Action(c.skListAction)
	skls.Arg("account", "Account to act on").StringVar(&c.accountName)
	skls.Flag("operator", "Operator to act on").StringVar(&c.operatorName)

	skrm := sk.Command("rm", "Remove a scoped signing key").Action(c.skRmAction)
	skrm.Arg("account", "Account to act on").StringVar(&c.accountName)
	skrm.Flag("key", "The key to remove").StringVar(&c.skRole)
	skrm.Flag("operator", "Operator to act on").StringVar(&c.operatorName)
	skrm.Flag("force", "Removes without prompting").Short('f').UnNegatableBoolVar(&c.force)

	mappings := acct.Command("mappings", "Manage account level subject mapping and partitioning").Alias("m").Alias("mapping").Alias("map")

	mappingsaAdd := mappings.Command("add", "Add a new mapping").Alias("new").Alias("a").Action(c.mappingAddAction)
	mappingsaAdd.Arg("account", "Account to create the mappings on").StringVar(&c.accountName)
	mappingsaAdd.Arg("source", "The source subject of the mapping").StringVar(&c.mapSource)
	mappingsaAdd.Arg("target", "The target subject of the mapping").StringVar(&c.mapTarget)
	mappingsaAdd.Arg("weight", "The weight (%) of the mapping").Default("100").UintVar(&c.mapWeight)
	mappingsaAdd.Arg("cluster", "Limit the mappings to a specific cluster").StringVar(&c.mapCluster)
	mappingsaAdd.Flag("operator", "Operator to act on").StringVar(&c.operatorName)
	mappingsaAdd.Flag("config", "json or yaml file to read configuration from").ExistingFileVar(&c.inputFile)

	mappingsls := mappings.Command("ls", "List mappings").Alias("list").Action(c.mappingListAction)
	mappingsls.Arg("account", "Account to list the mappings from").StringVar(&c.accountName)
	mappingsls.Flag("operator", "Operator to act on").StringVar(&c.operatorName)

	mappingsrm := mappings.Command("rm", "Remove a mapping").Action(c.mappingRmAction)
	mappingsrm.Arg("account", "Account to remove the mappings from").StringVar(&c.accountName)
	mappingsrm.Arg("source", "The source subject of the mapping").StringVar(&c.mapSource)
	mappingsrm.Flag("operator", "Operator to act on").StringVar(&c.operatorName)

	mappingsinfo := mappings.Command("info", "Show information about a mapping").Alias("i").Alias("show").Alias("view").Action(c.mappingInfoAction)
	mappingsinfo.Arg("account", "Account to inspect the mappings from").StringVar(&c.accountName)
	mappingsinfo.Arg("source", "The source subject of the mapping").StringVar(&c.mapSource)
	mappingsinfo.Flag("operator", "Operator to act on").StringVar(&c.operatorName)
}

func (c *authAccountCommand) selectAccount(pick bool) (*ab.AuthImpl, ab.Operator, ab.Account, error) {
	auth, oper, acct, err := au.SelectOperatorAccount(c.operatorName, c.accountName, pick)
	if err != nil {
		return nil, nil, nil, err
	}

	c.operatorName = oper.Name()
	c.accountName = acct.Name()

	return auth, oper, acct, nil
}

func (c *authAccountCommand) selectOperator(pick bool) (*ab.AuthImpl, ab.Operator, error) {
	auth, oper, err := au.SelectOperator(c.operatorName, pick, true)
	if err != nil {
		return nil, nil, err
	}

	c.operatorName = oper.Name()

	return auth, oper, err
}

func (c *authAccountCommand) queryAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	_, oper, err := au.SelectOperator(c.operatorName, true, true)
	if err != nil {
		return err
	}

	acct, err := au.SelectAccount(oper, c.accountName, "")
	if err != nil {
		return err
	}

	var token string
	err = doReqAsync(nil, fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.CLAIMS.LOOKUP", acct.Subject()), 1, nc, func(b []byte) {
		token = string(b)
	})
	if err != nil {
		return err
	}

	if token == "" {
		return fmt.Errorf("did not receive a valid token from the server")
	}

	if c.output != "" {
		err = os.WriteFile(c.output, []byte(token), 0600)
		if err != nil {
			return err
		}
	}

	acct, err = ab.NewAccountFromJWT(token)
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, nil, acct)
}

func (c *authAccountCommand) pushAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.showJWT {
		fmt.Printf("Account JWT for %s\n", c.accountName)
		fmt.Println()
		fmt.Println(acct.JWT())
		fmt.Println()
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	expect, _ := currentActiveServers(nc)
	if expect > 0 {
		fmt.Printf("Updating account %s (%s) on %d server(s)\n", acct.Name(), acct.Subject(), expect)
	} else {
		fmt.Printf("Updating Account %s (%s) on all servers\n", acct.Name(), acct.Subject())
	}
	fmt.Println()

	errStr := color.RedString("X")
	okStr := color.GreenString("✓")
	updated := 0
	failed := 0

	subj := fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.CLAIMS.UPDATE", acct.Subject())
	err = doReqAsync(acct.JWT(), subj, expect, nc, func(msg []byte) {
		update := server.ServerAPIClaimUpdateResponse{}
		err = json.Unmarshal(msg, &update)
		if err != nil {
			fmt.Printf("%s Invalid JSON response received: %v: %s\n", errStr, err, string(msg))
			failed++
			return
		}

		if update.Error != nil {
			fmt.Printf("%s Update failed on %s: %v\n", errStr, update.Server.Name, update.Error.Description)
			failed++
			return
		}

		fmt.Printf("%s Update completed on %s\n", okStr, update.Server.Name)
		updated++
	})
	if err != nil {
		return err
	}

	fmt.Println()
	if expect > 0 {
		fmt.Printf("Success %d Failed %d Expected %d\n", updated, failed, expect)
	} else {
		fmt.Printf("Success %d Failed %d\n", updated, failed)
	}

	if failed > 0 {
		if expect > 0 {
			return fmt.Errorf("update failed on %d/%d servers", failed, expect)
		}
		return fmt.Errorf("update failed on %d servers", failed)
	}

	if updated == 0 {
		return fmt.Errorf("no servers were updated")
	}

	if expect > 0 && (updated+failed != expect) {
		return fmt.Errorf("received updated from only %d out of %d servers", updated+failed, expect)
	}

	return nil
}

func (c *authAccountCommand) skRmAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	sk, err := au.SelectSigningKey(acct, c.skRole)
	if err != nil {
		return err
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove the Scoped Signing Key %s with role %s", sk.Key(), sk.Role()), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	ok, err := acct.ScopedSigningKeys().Delete(sk.Key())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("key %q not found", sk.Key())
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Printf("key %q removed\n", sk.Key())

	return nil
}

func (c *authAccountCommand) skInfoAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	sk, err := au.SelectSigningKey(acct, c.skRole)
	if err != nil {
		return err
	}

	out, err := c.showSk(sk)
	if err != nil {
		return err
	}

	fmt.Println(out)
	return nil
}

func (c *authAccountCommand) skAddAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.skRole == "" {
		err := iu.AskOne(&survey.Input{
			Message: "Role Name",
			Help:    "The role to associate with this key",
		}, &c.skRole, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if c.maxPayloadString != "" {
		c.maxPayload, err = iu.ParseStringAsBytes(c.maxPayloadString)
		if err != nil {
			return err
		}
	}

	scope, err := acct.ScopedSigningKeys().AddScope(c.skRole)
	if err != nil {
		return err
	}

	if c.description != "" {
		err = scope.SetDescription(c.description)
		if err != nil {
			return err
		}
	}

	limits := scope.(au.UserLimitsManager).UserPermissionLimits()
	limits.Subs = c.maxSubs
	limits.Payload = c.maxPayload
	limits.BearerToken = c.bearerAllowed
	limits.Locale = c.locale
	limits.Pub.Allow = c.pubAllow
	limits.Pub.Deny = c.pubDeny
	limits.Sub.Allow = c.subAllow
	limits.Sub.Deny = c.subDeny
	if len(c.connTypes) > 0 {
		limits.AllowedConnectionTypes = c.connectionTypes()
	}

	err = scope.(au.UserLimitsManager).SetUserPermissionLimits(limits)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowSk(os.Stdout, scope)
}

func (c *authAccountCommand) fShowSk(w io.Writer, limits ab.ScopeLimits) error {
	out, err := c.showSk(limits)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)

	return err
}

func (c *authAccountCommand) showSk(limits ab.ScopeLimits) (string, error) {
	cols := newColumns("Scoped Signing Key %s", limits.Key())

	cols.AddSectionTitle("Config")

	cols.AddRowIfNotEmpty("Description", limits.Description())
	cols.AddRow("Key", limits.Key())
	cols.AddRow("Role", limits.Role())

	err := au.RenderUserLimits(limits, cols)
	if err != nil {
		return "", err
	}

	return cols.Render()
}

func (c *authAccountCommand) connectionTypes() []string {
	var types []string

	for _, t := range c.connTypes {
		switch t {
		case "nats":
			types = append(types, "STANDARD")
		case "ws":
			types = append(types, "WEBSOCKET")
		case "wsleaf":
			types = append(types, "LEAFNODE_WS")
		case "leaf":
			types = append(types, "LEAFNODE")
		case "mqtt":
			types = append(types, "MQTT")
		}
	}

	return types
}

func (c *authAccountCommand) skListAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	var table *iu.Table

	if len(acct.ScopedSigningKeys().List()) > 0 {
		table = iu.NewTableWriter(opts(), "Scoped Signing Keys")
		table.AddHeaders("Role", "Key", "Description", "Max Subscriptions", "Pub Perms", "Sub Perms")
		for _, sk := range acct.ScopedSigningKeys().List() {
			scope, _ := acct.ScopedSigningKeys().GetScope(sk)

			pubs := len(scope.PubPermissions().Allow()) + len(scope.PubPermissions().Deny())
			subs := len(scope.SubPermissions().Allow()) + len(scope.SubPermissions().Deny())

			table.AddRow(scope.Role(), scope.Key(), scope.Description(), scope.MaxSubscriptions(), pubs, subs)
		}
		fmt.Println(table.Render())
		fmt.Println()
	}

	if table == nil {
		fmt.Println("No Scoped Signing Keys or Roles defined")
	}

	return nil
}

func (c *authAccountCommand) editAction(_ *fisk.ParseContext) error {
	auth, operator, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	jsEnabled := acct.Limits().JetStream().IsJetStreamEnabled()
	limits := acct.Limits().(au.OperatorLimitsManager).OperatorLimits()
	// copy existing settings into the flag settings so parsing treats those as defaults unless users set values
	if c.maxPayloadString == "" {
		c.maxPayloadString = strconv.Itoa(int(limits.Payload))
	}
	if !c.maxConnsIsSet {
		c.maxConns = limits.Conn
	}
	if !c.maxSubIsSet {
		c.maxSubs = limits.Subs
	}
	if !c.maxLeafNodesIsSet {
		c.maxLeafnodes = limits.LeafNodeConn
	}
	if !c.maxExportsIsSet {
		c.maxExports = limits.Exports
	}
	if !c.maxImportsIsSet {
		c.maxImports = limits.Imports
	}
	if !c.bearerAllowedIsSet {
		c.bearerAllowed = !limits.DisallowBearer
	}

	err = au.UpdateTags(acct.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	if jsEnabled {
		if !c.jetStreamIsSet {
			c.jetStream = true
		}

		jsl := limits.JetStreamLimits
		if c.storeMaxString == "" {
			c.storeMaxString = strconv.Itoa(int(jsl.DiskStorage))
		}
		if c.memMaxString == "" {
			c.memMaxString = strconv.Itoa(int(jsl.MemoryStorage))
		}
		if c.memMaxStreamString == "" {
			c.memMaxStreamString = strconv.Itoa(int(jsl.MemoryMaxStreamBytes))
		}
		if c.storeMaxStreamString == "" {
			c.storeMaxStreamString = strconv.Itoa(int(jsl.DiskMaxStreamBytes))
		}
		if !c.maxConsumersIsSet {
			c.maxConsumers = jsl.Consumer
		}
		if !c.maxStreamsIsSet {
			c.maxStreams = jsl.Streams
		}
		if !c.streamSizeRequiredIsSet {
			c.streamSizeRequired = jsl.MaxBytesRequired
		}
		if !c.maxAckPendingIsSet {
			c.maxAckPending = jsl.MaxAckPending
		}
	}

	err = c.parseStringOptions()
	if err != nil {
		return err
	}

	err = c.updateAccount(acct, c.jetStreamIsSet || jsEnabled)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, operator, acct)
}

//func (c *authAccountCommand) rmAction(_ *fisk.ParseContext) error {
//	fmt.Println("WARNING: At present deleting is not supported by the nsc store")
//	fmt.Println()
//
//	auth, operator, account, err := c.selectAccount(true)
//	if err != nil {
//		return err
//	}
//
//	if !c.force {
//		ok, err := askConfirmation(fmt.Sprintf("Really remove the Accouint %s", c.accountName), false)
//		if err != nil {
//			return err
//		}
//
//		if !ok {
//			return nil
//		}
//	}
//
//	err = operator.Accounts().Delete(account.Name())
//	if err != nil {
//		return err
//	}
//
//	err = auth.Commit()
//	if err != nil {
//		return err
//	}
//
//	fmt.Printf("Removed account %s\n", account.Name())
//	return nil
//}

func (c *authAccountCommand) lsAction(_ *fisk.ParseContext) error {
	_, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	list := operator.Accounts().List()
	if len(list) == 0 {
		fmt.Println("No Accounts found")
		return nil
	}

	if c.listNames {
		for _, op := range list {
			fmt.Println(op.Name())
		}
		return nil
	}

	table := iu.NewTableWriter(opts(), "Accounts")
	table.AddHeaders("Name", "Subject", "Users", "JetStream", "System")
	for _, acct := range list {
		system := ""
		js := ""
		sa, err := operator.SystemAccount()
		if err == nil && sa != nil && acct.Subject() == sa.Subject() {
			system = "true"
		}
		if acct.Limits().JetStream().IsJetStreamEnabled() {
			js = "true"
		}

		table.AddRow(acct.Name(), acct.Subject(), len(acct.Users().List()), js, system)
	}
	fmt.Println(table.Render())

	return nil
}

func (c *authAccountCommand) infoAction(_ *fisk.ParseContext) error {
	_, operator, account, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, operator, account)
}

func (c *authAccountCommand) updateAccount(acct ab.Account, js bool) error {
	limits := acct.Limits().(au.OperatorLimitsManager).OperatorLimits()
	limits.Conn = c.maxConns
	limits.Subs = c.maxSubs
	limits.Payload = c.maxPayload
	limits.LeafNodeConn = c.maxLeafnodes
	limits.Exports = c.maxExports
	limits.Imports = c.maxImports
	limits.DisallowBearer = !c.bearerAllowed
	if js {
		if c.storeMaxStream > 0 {
			limits.JetStreamLimits.DiskMaxStreamBytes = c.storeMaxStream
		}
		if c.memMaxStream > 0 {
			limits.JetStreamLimits.MemoryMaxStreamBytes = c.memMaxStream
		}
		limits.JetStreamLimits.DiskStorage = c.storeMax
		limits.JetStreamLimits.MemoryStorage = c.memMax
		limits.JetStreamLimits.MaxBytesRequired = c.streamSizeRequired
		limits.JetStreamLimits.Consumer = c.maxConsumers
		limits.JetStreamLimits.Streams = c.maxStreams
		limits.JetStreamLimits.MaxAckPending = c.maxAckPending
	}

	err := acct.Limits().(au.OperatorLimitsManager).SetOperatorLimits(limits)
	if err != nil {
		return err
	}

	if c.expiry > 0 {
		err = acct.SetExpiry(time.Now().Add(c.expiry).Unix())
		if err != nil {
			return err
		}
	}

	if c.clusterTrafficIsSet {
		err = acct.SetClusterTraffic(c.clusterTraffic)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *authAccountCommand) parseStringOptions() error {
	var err error

	if c.maxPayloadString != "" {
		c.maxPayload, err = iu.ParseStringAsBytes(c.maxPayloadString)
		if err != nil {
			return err
		}
	}

	if c.jetStream {
		if c.storeMaxString == "" {
			c.storeMax, err = askOneBytes("Maximum JetStream Disk Storage", "1GB", "Maximum amount of disk this account may use, set using --js-disk", "JetStream requires maximum Disk usage set")
			if err != nil {
				return err
			}
		}
		if c.memMaxString == "" {
			c.memMax, err = askOneBytes("Maximum JetStream Memory Storage", "1GB", "Maximum amount of memory this account may use, set using --js-memory", "JetStream requires maximum Memory usage set")
			if err != nil {
				return err
			}
		}

		if c.storeMaxString != "" {
			c.storeMax, err = iu.ParseStringAsBytes(c.storeMaxString)
			if err != nil {
				return err
			}
		}
		if c.memMaxString != "" {
			c.memMax, err = iu.ParseStringAsBytes(c.memMaxString)
			if err != nil {
				return err
			}
		}

		if c.memMaxStreamString != "-1" {
			c.memMaxStream, err = iu.ParseStringAsBytes(c.memMaxStreamString)
			if err != nil {
				return err
			}
		}
		if c.storeMaxStreamString != "-1" {
			c.storeMaxStream, err = iu.ParseStringAsBytes(c.storeMaxStreamString)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *authAccountCommand) addAction(_ *fisk.ParseContext) error {
	auth, operator, err := c.selectOperator(true)
	if err != nil {
		return err
	}

	if c.accountName == "" {
		err := iu.AskOne(&survey.Input{
			Message: "Account Name",
			Help:    "A unique name for the Account being added",
		}, &c.accountName, survey.WithValidator(survey.Required))
		if err != nil {
			return err
		}
	}

	if au.IsAuthItemKnown(operator.Accounts().List(), c.accountName) {
		return fmt.Errorf("account %s already exist", c.accountName)
	}

	acct, err := operator.Accounts().Add(c.accountName)
	if err != nil {
		return err
	}

	if c.signingKey != "" {
		sk, err := au.SelectSigningKey(acct, c.signingKey)
		if err != nil {
			return err
		}
		c.signingKey = sk.Key()
	}

	if c.signingKey != "" {
		err = acct.SetIssuer(c.signingKey)
		if err != nil {
			return err
		}
	}

	err = au.UpdateTags(acct.Tags(), c.tags, c.rmTags)
	if err != nil {
		return err
	}

	if !c.defaults {
		if c.maxConns == -1 {
			c.maxConns, err = askOneInt("Maximum Connections", "-1", "The maximum amount of client connections allowed for this account, set using --connections")
			if err != nil {
				return err
			}
		}

		if c.maxSubs == -1 {
			c.maxSubs, err = askOneInt("Maximum Subscriptions", "-1", "The maximum amount of subscriptions allowed for this account, set using --subscriptions")
			if err != nil {
				return err
			}
		}

		if c.maxPayloadString == "" {
			c.maxPayload, err = askOneBytes("Maximum Message Payload", "-1", "The maximum size any message may have, set using --payload", "")
			if err != nil {
				return err
			}
			c.maxPayloadString = ""
		}

		fmt.Println()
	}

	err = c.parseStringOptions()
	if err != nil {
		return err
	}

	err = c.updateAccount(acct, c.jetStream)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowAccount(os.Stdout, operator, acct)
}

func (c *authAccountCommand) fShowAccount(w io.Writer, operator ab.Operator, acct ab.Account) error {
	out, err := c.showAccount(operator, acct)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)

	return err
}

func (c *authAccountCommand) showAccount(operator ab.Operator, acct ab.Account) (string, error) {
	limits := acct.Limits()
	js := limits.JetStream()
	serviceExports := len(acct.Exports().Services().List())
	streamExports := len(acct.Exports().Streams().List())
	serviceImports := len(acct.Imports().Services().List())
	streamImports := len(acct.Imports().Streams().List())

	cols := newColumns("Account %s (%s)", acct.Name(), acct.Subject())

	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", acct.Name())
	cols.AddRow("Issuer", acct.Issuer())
	if operator != nil {
		cols.AddRow("Operator", operator.Name())
		sa, err := operator.SystemAccount()
		if err == nil {
			cols.AddRow("System Account", sa.Subject() == acct.Subject())
		} else {
			cols.AddRow("System Account", false)
		}
	}
	if tags, _ := acct.Tags().All(); len(tags) > 0 {
		cols.AddStringsAsValue("Tags", tags)
	}
	cols.AddRow("JetStream", js.IsJetStreamEnabled())
	cols.AddRowIf("Expiry", time.Unix(acct.Expiry(), 0), acct.Expiry() > 0)
	cols.AddRow("Users", len(acct.Users().List()))
	cols.AddRow("Revocations", len(acct.Revocations().List()))
	cols.AddRow("Service Exports", serviceExports)
	cols.AddRow("Stream Exports", streamExports)
	cols.AddRow("Service Imports", serviceImports)
	cols.AddRow("Stream Imports", streamImports)

	cols.AddSectionTitle("Limits")
	cols.AddRow("Bearer Tokens Allowed", !limits.DisallowBearerTokens())
	cols.AddRowUnlimited("Subscriptions", limits.MaxSubscriptions(), -1)
	cols.AddRowUnlimited("Connections", limits.MaxConnections(), -1)
	cols.AddRowUnlimitedIf("Maximum Payload", humanize.IBytes(uint64(limits.MaxPayload())), limits.MaxPayload() <= 0)
	if limits.MaxData() > 0 {
		cols.AddRow("Data", limits.MaxData()) // only showing when set as afaik its a ngs thing
	}
	cols.AddRowUnlimited("Leafnodes", limits.MaxLeafNodeConnections(), -1)
	cols.AddRowUnlimited("Imports", limits.MaxImports(), -1)
	cols.AddRowUnlimited("Exports", limits.MaxExports(), -1)

	if js.IsJetStreamEnabled() {
		cols.Indent(2)
		cols.AddSectionTitle("JetStream Settings")

		traffic := acct.ClusterTraffic()
		if traffic == "" {
			traffic = "system"
		}
		cols.AddRow("Cluster Traffic", traffic)

		tiers := c.validTiers(acct)

		cols.Indent(4)
		for _, tc := range tiers {
			tier, _ := js.Get(tc)
			if tier == nil {
				continue
			}

			if tc == 0 {
				cols.AddSectionTitle("Account Default Limits")
			} else {
				cols.AddSectionTitle("Tier %d", tc)
			}

			if unlimited, _ := tier.IsUnlimited(); unlimited {
				cols.Indent(6)
				cols.Println("Unlimited")
				cols.Indent(4)
				continue
			}

			maxAck, _ := tier.MaxAckPending()
			maxMem, _ := tier.MaxMemoryStorage()
			maxMemStream, _ := tier.MaxMemoryStreamSize()
			maxConns, _ := tier.MaxConsumers()
			maxDisk, _ := tier.MaxDiskStorage()
			maxDiskStream, _ := tier.MaxDiskStreamSize()
			streams, _ := tier.MaxStreams()
			streamSizeRequired, _ := tier.MaxStreamSizeRequired()

			cols.AddRowUnlimited("Max Ack Pending", maxAck, 0)
			cols.AddRowUnlimited("Maximum Streams", streams, -1)
			cols.AddRowUnlimited("Max Consumers", maxConns, -1)
			cols.AddRow("Max Stream Size Required", streamSizeRequired)
			cols.AddRow("Max File Storage", humanize.IBytes(uint64(maxDisk)))
			cols.AddRowIf("Max File Storage Stream Size", humanize.IBytes(uint64(maxDiskStream)), maxDiskStream > 0)
			cols.AddRow("Max Memory Storage", humanize.IBytes(uint64(maxMem)))
			cols.AddRowIf("Max Memory Storage Stream Size", humanize.IBytes(uint64(maxMemStream)), maxMemStream > 0)
		}

		cols.Indent(0)
	}

	return cols.Render()
}

func (c *authAccountCommand) validTiers(acct ab.Account) []int8 {
	tiers := []int8{}
	for i := int8(0); i <= 5; i++ {
		tier, _ := acct.Limits().JetStream().Get(i)
		if tier != nil {
			tiers = append(tiers, i)
		}
	}

	if len(tiers) > 1 {
		tiers = tiers[1:]
	}

	return tiers
}

func (c *authAccountCommand) loadMappingsConfig() (map[string][]ab.Mapping, error) {
	if c.inputFile != "" {
		f, err := os.ReadFile(c.inputFile)
		if err != nil {
			return nil, err
		}

		var mappings map[string][]ab.Mapping
		err = yaml.Unmarshal(f, &mappings)
		if err != nil {
			return nil, fmt.Errorf("unable to load config file: %s", err)
		}
		return mappings, nil
	}
	return nil, nil

}

func (c *authAccountCommand) mappingAddAction(_ *fisk.ParseContext) error {
	var err error
	mappings := map[string][]ab.Mapping{}
	if c.inputFile != "" {
		mappings, err = c.loadMappingsConfig()
		if err != nil {
			return err
		}
	}

	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.inputFile == "" {
		if c.mapSource == "" {
			err := iu.AskOne(&survey.Input{
				Message: "Source subject",
				Help:    "The source subject of the mapping",
			}, &c.mapSource, survey.WithValidator(survey.Required))
			if err != nil {
				return err
			}
		}

		if c.mapTarget == "" {
			err := iu.AskOne(&survey.Input{
				Message: "Target subject",
				Help:    "The target subject of the mapping",
			}, &c.mapTarget, survey.WithValidator(survey.Required))
			if err != nil {
				return err
			}
		}

		mapping := ab.Mapping{Subject: c.mapTarget, Weight: uint8(c.mapWeight)}
		if c.mapCluster != "" {
			mapping.Cluster = c.mapCluster
		}
		// check if there are mappings already set for the source
		currentMappings := acct.SubjectMappings().Get(c.mapSource)
		if len(currentMappings) > 0 {
			// Check that we don't overwrite the current mapping
			for _, m := range currentMappings {
				if m.Subject == c.mapTarget {
					return fmt.Errorf("mapping %s -> %s already exists", c.mapSource, c.mapTarget)
				}
			}
		}
		currentMappings = append(currentMappings, mapping)
		mappings[c.mapSource] = currentMappings
	}

	for subject, m := range mappings {
		err = acct.SubjectMappings().Set(subject, m...)
		if err != nil {
			return err
		}
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowMappings(os.Stdout, mappings)
}

func (c *authAccountCommand) mappingInfoAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	accountMappings := acct.SubjectMappings().List()
	if len(accountMappings) == 0 {
		fmt.Println("No mappings defined")
		return nil
	}

	if c.mapSource == "" {
		err = iu.AskOne(&survey.Select{
			Message:  "Select a mapping to inspect",
			Options:  accountMappings,
			PageSize: iu.SelectPageSize(len(accountMappings)),
		}, &c.mapSource)
		if err != nil {
			return err
		}
	}

	mappings := map[string][]ab.Mapping{
		c.mapSource: acct.SubjectMappings().Get(c.mapSource),
	}

	return c.fShowMappings(os.Stdout, mappings)
}

func (c *authAccountCommand) mappingListAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	mappings := acct.SubjectMappings().List()
	if len(mappings) == 0 {
		fmt.Println("No mappings defined")
		return nil
	}

	tbl := iu.NewTableWriter(opts(), "Subject mappings for account %s", acct.Name())
	tbl.AddHeaders("Source Subject", "Target Subject", "Weight", "Cluster")

	for _, fromMapping := range acct.SubjectMappings().List() {
		subjectMaps := acct.SubjectMappings().Get(fromMapping)
		for _, m := range subjectMaps {
			tbl.AddRow(fromMapping, m.Subject, m.Weight, m.Cluster)
		}
	}

	fmt.Println(tbl.Render())
	return nil
}

func (c *authAccountCommand) mappingRmAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	mappings := acct.SubjectMappings().List()
	if len(mappings) == 0 {
		fmt.Println("No mappings defined")
		return nil
	}

	if c.mapSource == "" {
		err = iu.AskOne(&survey.Select{
			Message:  "Select a mapping to delete",
			Options:  mappings,
			PageSize: iu.SelectPageSize(len(mappings)),
		}, &c.mapSource)
		if err != nil {
			return err
		}
	}

	err = acct.SubjectMappings().Delete(c.mapSource)
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	fmt.Printf("Deleted mapping {%s}\n", c.mapSource)
	return nil
}

func (c *authAccountCommand) fShowMappings(w io.Writer, mappings map[string][]ab.Mapping) error {
	out, err := c.showMappings(mappings)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *authAccountCommand) showMappings(mappings map[string][]ab.Mapping) (string, error) {
	cols := newColumns("Subject mappings")
	cols.AddSectionTitle("Configuration")
	for source, m := range mappings {
		totalWeight := 0
		for _, wm := range m {
			cols.AddRow("Source", source)
			cols.AddRow("Target", wm.Subject)
			cols.AddRow("Weight", wm.Weight)
			cols.AddRow("Cluster", wm.Cluster)
			cols.AddRow("", "")
			totalWeight += int(wm.Weight)
		}
		cols.AddRow("Total weight:", totalWeight)
		cols.AddRow("", "")
	}

	return cols.Render()
}
