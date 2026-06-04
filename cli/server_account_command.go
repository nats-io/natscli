package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	iu "github.com/nats-io/natscli/internal/util"

	"github.com/nats-io/jsm.go/serverdata"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/columns"

	"github.com/choria-io/fisk"
)

type srvAccountCommand struct {
	json        bool
	account     string
	server      string
	force       bool
	archivePath string
}

func configureServerAccountCommand(srv *fisk.CmdClause) {
	c := &srvAccountCommand{}

	account := srv.Command("account", "Interact with accounts").Alias("acct")
	account.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	info := account.Command("info", "Shows information for an account").Alias("i").Action(c.infoAction)
	info.Tag("scope:system", "impact:ro")
	info.Arg("account", "The name of the account to view").Required().StringVar(&c.account)
	info.Flag("host", "Request information from a specific server").StringVar(&c.server)
	info.Flag("archive", "Read data from an archive file").StringVar(&c.archivePath)

	purge := account.Command("purge", "Purge assets from JetStream clusters").Action(c.purgeAccount)
	purge.Tag("scope:system", "impact:rw")
	purge.Arg("account", "The name of the account to purge").PlaceHolder("NAME").Required().StringVar(&c.account)
	purge.Flag("force", "Perform the operation without prompting").Short('f').UnNegatableBoolVar(&c.force)
}

func (c *srvAccountCommand) purgeAccount(_ *fisk.ParseContext) error {
	if !c.force {
		fmt.Printf("This operation deletes all data from the %s account and cannot be reversed.\n\n", c.account)
		remove, err := askConfirmation(fmt.Sprintf("Really purge account %s", c.account), false)
		if err != nil {
			return err
		}

		if !remove {
			return nil
		}
	}

	_, mgr, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	err = mgr.MetaPurgeAccount(c.account)
	if err != nil {
		return err
	}

	fmt.Printf("Purge operation on account %s initiated\n", c.account)

	return nil
}

func (c *srvAccountCommand) dataSource() (serverdata.Source, error) {
	if c.archivePath != "" {
		return serverdata.NewAuditArchive(c.archivePath)
	}

	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return nil, err
	}
	reqFn := func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		return serverdata.DoReq(ctx, req, subj, waitFor, nc, opts().Timeout, traceLogger())
	}
	return serverdata.NewLive(nc, reqFn, 1)
}

func (c *srvAccountCommand) infoAction(_ *fisk.ParseContext) error {
	ds, err := c.dataSource()
	if err != nil {
		return err
	}
	defer ds.Close()

	opts := server.AccountzEventOptions{
		AccountzOptions:    server.AccountzOptions{Account: c.account},
		EventFilterOptions: server.EventFilterOptions{Name: c.server, ExactMatch: true},
	}

	res, err := ds.Accountz(opts)
	if err != nil {
		return err
	}

	if len(res) == 0 {
		if c.archivePath != "" {
			if c.server != "" {
				return fmt.Errorf("no captured data for account %q on host %q in %s", c.account, c.server, c.archivePath)
			}
			return fmt.Errorf("no captured data for account %q in %s", c.account, c.archivePath)
		}
		return fmt.Errorf("no responses received, ensure the account used has system privileges and appropriate permissions")
	}

	resp := res[0]
	if resp.Error != nil {
		return fmt.Errorf("%s", resp.Error.Description)
	}

	if resp.Data == nil || resp.Data.Account == nil {
		return fmt.Errorf("no account information received")
	}

	nfo := resp.Data.Account

	if c.json {
		iu.PrintJSON(nfo)
		return nil
	}

	cols := newColumnsf("Account information for account %s", nfo.AccountName)
	defer cols.Frender(os.Stdout)

	cols.AddSectionTitle("Details")

	cols.AddRow("Complete", nfo.Complete)
	cols.AddRow("Expired", nfo.Expired)
	cols.AddRow("System Account", nfo.IsSystem)
	cols.AddRowf("Updated", "%v (%s ago)", f(nfo.LastUpdate), f(time.Since(nfo.LastUpdate)))
	cols.AddRow("JetStream", nfo.JetStream)
	cols.AddRowIfNotEmpty("Issuer", nfo.IssuerKey)
	cols.AddRowIfNotEmpty("Tag", nfo.NameTag)
	cols.AddRowIf("Tags", nfo.Tags, len(nfo.Tags) > 0)

	if len(nfo.Mappings) > 0 {
		cols.AddSectionTitle("Mappings")

		for subj, mappings := range nfo.Mappings {
			c.renderMappings(cols, subj, mappings)
		}
	}

	if len(nfo.Imports) > 0 {
		cols.AddSectionTitle("Imports")

		for _, imp := range nfo.Imports {
			c.renderImport(cols, imp)
		}
	}

	if len(nfo.Exports) > 0 {
		cols.AddSectionTitle("Exports")

		for _, exp := range nfo.Exports {
			c.renderExport(cols, exp)
		}
	}

	if len(nfo.Responses) > 0 {
		cols.AddSectionTitle("Responses")

		for _, imp := range nfo.Responses {
			c.renderImport(cols, imp)
		}
	}

	if len(nfo.RevokedUser) > 0 {
		cols.AddSectionTitle("Revoked Users")

		for r, t := range nfo.RevokedUser {
			cols.AddRowf(r, "%s (%s ago)", t, f(time.Since(t)))
		}
	}

	return nil
}

func (c *srvAccountCommand) renderMappings(cols *columns.Writer, subj string, mappings []*server.MapDest) {
	if len(mappings) == 0 {
		return
	}

	cols.Indent(2)
	cols.Println(subj)
	for _, mapping := range mappings {
		parts := []string{mapping.Subject}
		if mapping.Cluster != "" {
			parts = append(parts, fmt.Sprintf("in cluster %s", mapping.Cluster))
		}
		if mapping.Weight != 100 {
			parts = append(parts, fmt.Sprintf("with weight %d", mapping.Weight))
		}
		cols.Indent(4)
		cols.AddRow("Destination", parts)
		cols.Indent(2)
	}
	cols.Indent(0)
	cols.Println()
}

func (c *srvAccountCommand) renderExport(cols *columns.Writer, exp server.ExtExport) {
	cols.AddRow("Subject", exp.Subject)
	cols.AddRow("Type", exp.Type.String())
	cols.AddRow("Tokens Required", exp.TokenReq)
	cols.AddRow("Response Type", exp.ResponseType)
	cols.AddRowIf("Response Threshold", exp.ResponseThreshold, exp.ResponseThreshold > 0)
	cols.AddRowIfNotEmpty("Description", exp.Description)
	cols.AddRowIfNotEmpty("Info URL", exp.InfoURL)
	cols.AddRowIfNotEmpty("Name", exp.Name)
	cols.AddRowIf("Account Token Pos", exp.AccountTokenPosition, exp.AccountTokenPosition > 0)

	if len(exp.ApprovedAccounts) > 0 {
		if len(exp.ApprovedAccounts) > 20 {
			cols.AddRowf("Accounts", "Rendering 20/%d use --json for the full list", len(exp.ApprovedAccounts))
		} else {
			cols.AddRow("Accounts", "")
		}

		var cnt int
		for _, k := range exp.ApprovedAccounts {
			cols.AddRow("", k)
			if cnt == 19 {
				break
			}
			cnt++
		}
	}

	if len(exp.Revocations) > 0 {
		if len(exp.Revocations) > 20 {
			cols.AddRowf("Revocations", "Rendering 20/%d use --json for the full list", len(exp.Revocations))
		} else {
			cols.AddRow("Revocations", "")
		}
		cols.Println()

		var cnt int
		for k := range exp.Revocations {
			cols.AddRow("", k)
			if cnt == 19 {
				break
			}
			cnt++
		}
	}
	cols.Println()
}

func (c *srvAccountCommand) renderImport(cols *columns.Writer, imp server.ExtImport) {
	local := string(imp.LocalSubject)
	subj := string(imp.Subject)
	if local == "" {
		local = subj
	}
	if subj == local {
		subj = ""
	}

	// both are omitempty, i dont know what that is
	if local == "" {
		return
	}

	if imp.Invalid {
		cols.AddRow("Invalid", true)
	}

	parts := []string{local}
	if subj != "" {
		parts = append(parts, fmt.Sprintf("from subject %s", subj))
	}
	if imp.Account != "" {
		if subj == "" {
			parts = append(parts, fmt.Sprintf("from account %s", imp.Account))
		} else {
			parts = append(parts, fmt.Sprintf("in account %s", imp.Account))
		}
	}

	cols.AddRow("Subject", strings.Join(parts, " "))
	cols.AddRow("Type", imp.Type.String())
	cols.AddRowIfNotEmpty("Name", imp.Name)
	cols.AddRow("Sharing", imp.Share)
	if imp.TrackingHdr == nil {
		cols.AddRow("Tracking", imp.Tracking)
	} else {
		cols.AddRowf("Tracking", "%t header %v", imp.Tracking, imp.TrackingHdr)
	}
	cols.Println()
}
