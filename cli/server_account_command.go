package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/nats-io/nats-server/v2/server"
)

type srvAccountCommand struct {
	json    bool
	account string
	server  string
}

func configureServerAccountCommand(srv *fisk.CmdClause) {
	c := &srvAccountCommand{}

	account := srv.Command("account", "Interact with accounts").Alias("acct")
	account.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	info := account.Command("info", "Shows information for an account").Alias("i").Action(c.infoAction)
	info.Arg("account", "The name of the account to view").Required().StringVar(&c.account)
	info.Flag("host", "Request information from a specific server").StringVar(&c.server)
}

func (c *srvAccountCommand) infoAction(_ *fisk.ParseContext) error {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return err
	}

	opts := server.AccountzEventOptions{
		AccountzOptions:    server.AccountzOptions{Account: c.account},
		EventFilterOptions: server.EventFilterOptions{Name: c.server},
	}

	res, err := doReq(&opts, "$SYS.REQ.SERVER.PING.ACCOUNTZ", 1, nc)
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return fmt.Errorf("no responses received, ensure the account used has system privileges and appropriate permissions")
	}

	reqresp := map[string]json.RawMessage{}
	err = json.Unmarshal(res[0], &reqresp)
	if err != nil {
		return err
	}

	if errresp, ok := reqresp["error"]; ok {
		res := map[string]any{}
		err := json.Unmarshal(errresp, &res)
		if err != nil {
			return fmt.Errorf("invalid response received: %q", errresp)
		}

		msg, ok := res["description"]
		if !ok {
			return fmt.Errorf("%q", errresp)
		}

		return fmt.Errorf("%v", msg)
	}

	data, ok := reqresp["data"]
	if !ok {
		return fmt.Errorf("no data received in response: %#v", reqresp)
	}

	account := server.Accountz{}
	err = json.Unmarshal(data, &account)
	if err != nil {
		return err
	}

	if account.Account == nil {
		return fmt.Errorf("no account information received")
	}

	nfo := account.Account

	if c.json {
		printJSON(nfo)
		return nil
	}

	fmt.Printf("Account information for account %s\n\n", nfo.AccountName)

	fmt.Println("Details:")
	fmt.Println()

	fmt.Printf("        Complete: %t\n", nfo.Complete)
	fmt.Printf("         Expired: %t\n", nfo.Expired)
	fmt.Printf("  System Account: %t\n", nfo.IsSystem)
	fmt.Printf("         Updated: %v (%s ago)\n", nfo.LastUpdate.Format("2006-01-02T15:04:05"), humanizeDuration(time.Since(nfo.LastUpdate)))
	fmt.Printf("       JetStream: %t\n", nfo.JetStream)

	if nfo.IssuerKey != "" {
		fmt.Printf("          Issuer: %v\n", nfo.IssuerKey)
	}
	if nfo.NameTag != "" {
		fmt.Printf("             Tag: %v\n", nfo.NameTag)
	}
	if len(nfo.Tags) > 0 {
		fmt.Printf("            Tags: %s\n", strings.Join(nfo.Tags, ", "))
	}

	fmt.Println()
	if len(nfo.Mappings) > 0 {
		fmt.Println("Mappings:")

		for subj, mappings := range nfo.Mappings {
			c.renderMappings(subj, mappings)
		}
	}

	if len(nfo.Imports) > 0 {
		fmt.Println()
		fmt.Println("Imports:")

		for _, imp := range nfo.Imports {
			c.renderImport(&imp)
		}
	}

	if len(nfo.Exports) > 0 {
		fmt.Println()
		fmt.Println("Exports:")

		for _, exp := range nfo.Exports {
			c.renderExport(&exp)
		}
	}

	if len(nfo.Responses) > 0 {
		fmt.Println()
		fmt.Println("Responses:")

		for _, imp := range nfo.Responses {
			c.renderImport(&imp)
		}
	}

	if len(nfo.RevokedUser) > 0 {
		fmt.Println()
		fmt.Println("Revoked Users:")

		for r, t := range nfo.RevokedUser {
			fmt.Printf("               %s: %s (%s ago)\n", r, t, humanizeDuration(time.Since(t)))
		}
	}

	return nil
}

func (c *srvAccountCommand) renderMappings(subj string, mappings []*server.MapDest) {
	if len(mappings) == 0 {
		return
	}

	fmt.Printf("\n   %s:\n", subj)
	for _, mapping := range mappings {
		parts := []string{fmt.Sprintf("Destination: %v", mapping.Subject)}
		if mapping.Cluster != "" {
			parts = append(parts, fmt.Sprintf("in cluster %s", mapping.Cluster))
		}
		if mapping.Weight != 100 {
			parts = append(parts, fmt.Sprintf("with weight %d", mapping.Weight))
		}
		fmt.Printf("      %s\n", strings.Join(parts, " "))
	}
}

func (c *srvAccountCommand) renderExport(exp *server.ExtExport) {
	fmt.Println()
	fmt.Printf("             Subject: %s\n", exp.Subject)
	fmt.Printf("                Type: %s\n", exp.Type.String())
	fmt.Printf("     Tokens Required: %t\n", exp.TokenReq)
	fmt.Printf("       Response Type: %s\n", exp.ResponseType)
	if exp.ResponseThreshold > 0 {
		fmt.Printf("  Response Threshold: %s\n", humanizeDuration(exp.ResponseThreshold))
	}
	if exp.Description != "" {
		fmt.Printf("         Description: %s\n", exp.Description)
	}
	if exp.InfoURL != "" {
		fmt.Printf("            Info URL: %s\n", exp.InfoURL)
	}
	if exp.Name != "" {
		fmt.Printf("                Name: %s\n", exp.Name)
	}
	if exp.AccountTokenPosition > 0 {
		fmt.Printf("   Account Token Pos: %d\n", exp.AccountTokenPosition)
	}

	if len(exp.ApprovedAccounts) > 0 {
		fmt.Println()
		if len(exp.ApprovedAccounts) > 33 {
			fmt.Printf("            Accounts: Rendering 33/%d use --json for the full list\n", len(exp.ApprovedAccounts))
		} else {
			fmt.Printf("            Accounts:\n")
		}

		var list []string
		var cnt int
		for _, k := range exp.ApprovedAccounts {
			list = append(list, k)
			if cnt == 32 {
				break
			}
			cnt++
		}

		dumpStrings(list, 3, 22)
	}

	if len(exp.Revocations) > 0 {
		fmt.Println()

		if len(exp.Revocations) > 33 {
			fmt.Printf("         Revocations: Rendering 33/%d use --json for the full list\n", len(exp.Revocations))
		} else {
			fmt.Printf("         Revocations:\n")
		}

		var list []string
		var cnt int
		for k := range exp.Revocations {
			list = append(list, k)
			if cnt == 32 {
				break
			}
			cnt++
		}

		dumpStrings(list, 3, 22)
	}
}

func (c *srvAccountCommand) renderImport(imp *server.ExtImport) {
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

	fmt.Println()

	if imp.Invalid {
		fmt.Printf("       Invalid: true\n")
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

	fmt.Printf("       Subject: %v\n", strings.Join(parts, " "))
	fmt.Printf("          Type: %s\n", imp.Type.String())
	if imp.Name != "" {
		fmt.Printf("          Name: %v\n", imp.Name)
	}
	fmt.Printf("       Sharing: %t\n", imp.Share)
	if imp.TrackingHdr == nil {
		fmt.Printf("      Tracking: %t\n", imp.Tracking)
	} else {
		fmt.Printf("      Tracking: %t header %v\n", imp.Tracking, imp.TrackingHdr)
	}
}
