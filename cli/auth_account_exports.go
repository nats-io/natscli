// Copyright 2024 The NATS Authors
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
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/nats-io/natscli/internal/util"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	ab "github.com/synadia-io/jwt-auth-builder.go"
)

func (c *authAccountCommand) exportKvAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	services := []string{
		"$JS.API.STREAM.INFO.KV_%s",
		"$JS.API.DIRECT.GET.KV_%s.$KV.%s.>",
		"$JS.API.CONSUMER.CREATE.KV_%s.>",
		"$KV.%s.>",
	}

	streams := []string{
		"_INBOX.KV_%s.>",
	}

	for _, s := range services {
		subj := strings.ReplaceAll(s, "%s", c.bucketName)
		fmt.Printf("Exporting Service Subject: %s\n", subj)

		exp, err := ab.NewServiceExport(fmt.Sprintf("KV_%s", c.bucketName), subj)
		if err != nil {
			return err
		}

		exp.SetDescription(fmt.Sprintf("Export for KV Bucket %s", c.bucketName))

		err = acct.Exports().Services().AddWithConfig(exp)
		if err != nil {
			return err
		}
	}

	for _, s := range streams {
		subj := strings.ReplaceAll(s, "%s", c.bucketName)
		fmt.Printf("Exporting Stream Subject: %s\n", subj)

		exp, err := ab.NewStreamExport(fmt.Sprintf("KV_%s", c.bucketName), subj)
		if err != nil {
			return err
		}

		exp.SetDescription(fmt.Sprintf("Export for KV Bucket %s", c.bucketName))

		err = acct.Exports().Streams().AddWithConfig(exp)
		if err != nil {
			return err
		}
	}

	return auth.Commit()
}

func (c *authAccountCommand) findExport(account ab.Account, subject string) ab.Export {
	for _, exp := range account.Exports().Streams().List() {
		if exp.Subject() == subject {
			return exp
		}
	}
	for _, exp := range account.Exports().Services().List() {
		if exp.Subject() == subject {
			return exp
		}
	}

	return nil
}

func (c *authAccountCommand) exportBySubject(acct ab.Account) []ab.Export {
	var ret []ab.Export

	for _, svc := range acct.Exports().Streams().List() {
		ret = append(ret, svc)
	}
	for _, svc := range acct.Exports().Services().List() {
		ret = append(ret, svc)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Subject() < ret[j].Subject()
	})

	return ret
}

func (c *authAccountCommand) exportSubjects(export ab.Exports) []string {
	var known []string
	for _, exp := range export.Services().List() {
		known = append(known, exp.Subject())
	}
	for _, exp := range export.Streams().List() {
		known = append(known, exp.Subject())
	}

	sort.Strings(known)

	return known
}

func (c *authAccountCommand) fShowExport(w io.Writer, exp ab.Export) error {
	out, err := c.showExport(exp)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *authAccountCommand) showExport(exp ab.Export) (string, error) {
	_, isService := exp.(ab.ServiceExport)

	cols := newColumns("Export info for %s exporting %s", exp.Name(), exp.Subject())

	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", exp.Name())
	cols.AddRowIfNotEmpty("Description", exp.Description())
	if isService {
		cols.AddRow("Kind", "Service")
	} else {
		cols.AddRow("Kind", "Stream")
	}
	cols.AddRowIfNotEmpty("Info", exp.InfoURL())
	cols.AddRow("Subject", exp.Subject())
	cols.AddRow("Activation Required", exp.TokenRequired())
	cols.AddRow("Account Token Position", exp.AccountTokenPosition())
	cols.AddRow("Advertised", exp.IsAdvertised())

	cols.AddSectionTitle("Revocations")

	if len(exp.Revocations().List()) > 0 {
		for _, rev := range exp.Revocations().List() {
			cols.AddRow(f(rev.At()), rev.PublicKey())
		}
	} else {
		cols.Println()
		cols.Println("No revocations found")
	}

	return cols.Render()
}

func (c *authAccountCommand) exportRmAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	exp := c.findExport(acct, c.subject)
	if exp == nil {
		return fmt.Errorf("subject %q is not exported", c.subject)
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove the %s Export", exp.Subject()), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	switch exp.(type) {
	case ab.StreamExport:
		_, err = acct.Exports().Streams().Delete(c.subject)
		fmt.Printf("Removing Stream Export for subject %q\n", c.subject)
	case ab.ServiceExport:
		_, err = acct.Exports().Services().Delete(c.subject)
		fmt.Printf("Removing Service Export for subject %q\n", c.subject)
	}
	if err != nil {
		return err
	}

	return auth.Commit()
}

func (c *authAccountCommand) exportInfoAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.subject == "" {
		known := c.exportSubjects(acct.Exports())

		if len(known) == 0 {
			return fmt.Errorf("no exports defined")
		}

		err = util.AskOne(&survey.Select{
			Message:  "Select an Export",
			Options:  known,
			PageSize: util.SelectPageSize(len(known)),
		}, &c.subject)
		if err != nil {
			return err
		}
	}

	if c.subject == "" {
		return fmt.Errorf("subject is required")
	}

	exp := c.findExport(acct, c.subject)
	if exp == nil {
		return fmt.Errorf("unknown export")
	}

	return c.fShowExport(os.Stdout, exp)
}

func (c *authAccountCommand) exportEditAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	exp := c.findExport(acct, c.subject)
	if exp == nil {
		return fmt.Errorf("export for subject %q not found", c.subject)
	}

	if c.url != nil {
		err = exp.SetInfoURL(c.url.String())
		if err != nil {
			return err
		}
	}

	if c.descriptionIsSet {
		err = exp.SetDescription(c.description)
		if err != nil {
			return err
		}
	}

	if c.tokenPosition > 0 {
		err = exp.SetAccountTokenPosition(c.tokenPosition)
		if err != nil {
			return err
		}
	}

	if c.advertiseIsSet {
		err = exp.SetAdvertised(c.advertise)
		if err != nil {
			return err
		}
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowExport(os.Stdout, exp)
}

func (c *authAccountCommand) exportAddAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	var exp ab.Export

	if c.isService {
		exp, err = ab.NewServiceExport(c.exportName, c.subject)
	} else {
		exp, err = ab.NewStreamExport(c.exportName, c.subject)
	}
	if err != nil {
		return err
	}

	err = exp.SetAccountTokenPosition(c.tokenPosition)
	if err != nil {
		return err
	}
	err = exp.SetAdvertised(c.advertise)
	if err != nil {
		return err
	}
	err = exp.SetDescription(c.description)
	if err != nil {
		return err
	}
	if c.url != nil {
		err = exp.SetInfoURL(c.url.String())
		if err != nil {
			return err
		}
	}
	if c.isService {
		err = acct.Exports().Services().AddWithConfig(exp.(ab.ServiceExport))
	} else {
		err = acct.Exports().Streams().AddWithConfig(exp.(ab.StreamExport))
	}
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowExport(os.Stdout, exp)
}

func (c *authAccountCommand) exportLsAction(_ *fisk.ParseContext) error {
	_, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if len(acct.Exports().Services().List()) == 0 && len(acct.Exports().Streams().List()) == 0 {
		fmt.Println("No Exports defined")
		return nil
	}

	exports := c.exportBySubject(acct)

	tbl := util.NewTableWriter(opts(), "Exports for account %s", acct.Name())
	tbl.AddHeaders("Name", "Kind", "Subject", "Activation Required", "Advertised", "Token Position", "Revocations")

	for _, e := range exports {
		switch exp := e.(type) {
		case ab.StreamExport:
			tbl.AddRow(exp.Name(), "Stream", exp.Subject(), exp.TokenRequired(), exp.IsAdvertised(), exp.AccountTokenPosition(), f(len(exp.Revocations().List())))
		case ab.ServiceExport:
			tbl.AddRow(exp.Name(), "Service", exp.Subject(), exp.TokenRequired(), exp.IsAdvertised(), exp.AccountTokenPosition(), f(len(exp.Revocations().List())))
		}
	}

	fmt.Println(tbl.Render())
	return nil
}
