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

	au "github.com/nats-io/natscli/internal/auth"
	"github.com/nats-io/natscli/internal/util"

	"github.com/AlecAivazis/survey/v2"
	"github.com/choria-io/fisk"
	ab "github.com/synadia-io/jwt-auth-builder.go"
)

func (c *authAccountCommand) importKvAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	services := [][2]string{
		{"$JS.API.STREAM.INFO.KV_%s", "STREAM.INFO.KV_%s"},
		{"$JS.API.DIRECT.GET.KV_%s.$KV.%s.>", "DIRECT.GET.KV_%s.$KV.%s.>"},
		{"$JS.API.CONSUMER.CREATE.KV_%s.>", "CONSUMER.CREATE.KV_%s.>"},
		{"$KV.%s.>", "$KV.%s.>"},
	}
	streams := [][2]string{
		{"_INBOX.KV_%s.>", ""},
	}

	for _, subs := range services {
		subj := strings.ReplaceAll(subs[0], "%s", c.bucketName)
		target := subj
		if subs[1] != "" {
			target = fmt.Sprintf("%s.%s", c.prefix, strings.ReplaceAll(subs[1], "%s", c.bucketName))
		}

		fmt.Printf("Importing Service Subject: %s\n", target)

		imp, err := ab.NewServiceImport(fmt.Sprintf("KV_%s", c.bucketName), c.importAccount, subj)
		if err != nil {
			return err
		}
		err = imp.SetLocalSubject(target)
		if err != nil {
			return err
		}

		err = acct.Imports().Services().AddWithConfig(imp)
		if err != nil {
			return fmt.Errorf("could not add import: %v", err)
		}
	}

	for _, subs := range streams {
		subj := strings.ReplaceAll(subs[0], "%s", c.bucketName)
		target := subj
		if subs[1] != "" {
			target = fmt.Sprintf("%s.%s", c.prefix, strings.ReplaceAll(subs[1], "%s", c.bucketName))
		}

		fmt.Printf("Importing Stream Subject: %s\n", target)

		imp, err := ab.NewStreamImport(fmt.Sprintf("KV_%s", c.bucketName), c.importAccount, subj)
		if err != nil {
			return err
		}
		err = imp.SetLocalSubject(target)
		if err != nil {
			return err
		}

		err = acct.Imports().Streams().AddWithConfig(imp)
		if err != nil {
			return err
		}
	}

	err = auth.Commit()
	if err != nil {
		return fmt.Errorf("could not commit auth account: %v", err)
	}

	return nil
}

func (c *authAccountCommand) importAddAction(_ *fisk.ParseContext) error {
	auth, op, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	src, err := au.SelectAccount(op, c.importAccount, "Select the Source account")
	if err != nil {
		return fmt.Errorf("could not select source account: %v", err)
	}

	var imp ab.Import
	if c.isService {
		imp, err = ab.NewServiceImport(c.importName, acct.Subject(), c.subject)
	} else {
		imp, err = ab.NewStreamImport(c.importName, acct.Subject(), c.subject)
	}
	if err != nil {
		return fmt.Errorf("could not add import: %v", err)
	}

	err = imp.SetAccount(src.Subject())
	if err != nil {
		return err
	}

	if c.localSubject == "" {
		err = imp.SetLocalSubject(c.subject)
	} else {
		err = imp.SetLocalSubject(c.localSubject)
	}
	if err != nil {
		return err
	}

	err = imp.SetShareConnectionInfo(c.share)
	if err != nil {
		return err
	}

	if c.allowTrace && !c.isService {
		err = imp.(ab.StreamImport).SetShareConnectionInfo(true)
		if err != nil {
			return err
		}
	}

	if c.isService {
		err = acct.Imports().Services().AddWithConfig(imp)
	} else {
		err = acct.Imports().Streams().AddWithConfig(imp.(ab.StreamImport))
	}
	if err != nil {
		return err
	}

	err = auth.Commit()
	if err != nil {
		return fmt.Errorf("commit failed: %v", err)
	}

	return c.fShowImport(os.Stdout, imp, op)
}

func (c *authAccountCommand) importLsAction(_ *fisk.ParseContext) error {
	_, op, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if len(acct.Imports().Services().List()) == 0 && len(acct.Imports().Streams().List()) == 0 {
		fmt.Println("No Imports defined")
		return nil
	}

	imports := c.importsBySubject(acct)

	tbl := util.NewTableWriter(opts(), "Imports for account %s", acct.Name())
	tbl.AddHeaders("Name", "Kind", "Source", "Local Subject", "Remote Subject", "Allows Tracing", "Sharing Connection Info")

	for _, i := range imports {
		ls := i.Subject()
		if i.LocalSubject() != "" {
			ls = i.LocalSubject()
		}

		src := i.Account()
		srcAccount, err := op.Accounts().Get(i.Account())
		if err == nil && srcAccount != nil {
			src = srcAccount.Name()
		}

		switch imp := i.(type) {
		case ab.StreamImport:
			tbl.AddRow(imp.Name(), "Stream", src, ls, imp.Subject(), imp.AllowTracing(), imp.IsShareConnectionInfo())
		case ab.ServiceImport:
			tbl.AddRow(imp.Name(), "Service", src, ls, imp.Subject(), "", imp.IsShareConnectionInfo())
		}
	}

	fmt.Println(tbl.Render())

	return nil
}

func (c *authAccountCommand) findImport(account ab.Account, localSubject string) ab.Import {
	for _, imp := range account.Imports().Streams().List() {
		if imp.LocalSubject() == localSubject {
			return imp
		}
	}
	for _, imp := range account.Imports().Services().List() {
		if imp.LocalSubject() == localSubject {
			return imp
		}
	}

	return nil
}

func (c *authAccountCommand) importsBySubject(acct ab.Account) []ab.Import {
	var ret []ab.Import

	for _, svc := range acct.Imports().Streams().List() {
		ret = append(ret, svc)
	}
	for _, svc := range acct.Imports().Services().List() {
		ret = append(ret, svc)
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Subject() < ret[j].Subject()
	})

	return ret
}

func (c *authAccountCommand) importInfoAction(_ *fisk.ParseContext) error {
	_, op, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	if c.subject == "" {
		known := c.importSubjects(acct.Imports())

		if len(known) == 0 {
			return fmt.Errorf("no imports defined")
		}

		err = util.AskOne(&survey.Select{
			Message:  "Select an Import",
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

	imp := c.findImport(acct, c.subject)
	if imp == nil {
		return fmt.Errorf("unknown import")
	}

	return c.fShowImport(os.Stdout, imp, op)
}

func (c *authAccountCommand) importEditAction(_ *fisk.ParseContext) error {
	auth, op, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	imp := c.findImport(acct, c.subject)
	if imp == nil {
		return fmt.Errorf("import for local subject %q not found", c.subject)
	}

	streamImport, isStream := imp.(ab.StreamImport)
	if c.allowTraceIsSet {
		if !isStream {
			return fmt.Errorf("service imports cannot allow tracing")
		}

		err = streamImport.SetAllowTracing(c.allowTrace)
		if err != nil {
			return err
		}
	}

	if c.shareIsSet {
		err = imp.SetShareConnectionInfo(c.share)
		if err != nil {
			return err
		}
	}

	if c.localSubject != "" {
		err = imp.SetLocalSubject(c.localSubject)
		if err != nil {
			return err
		}
	}

	err = auth.Commit()
	if err != nil {
		return err
	}

	return c.fShowImport(os.Stdout, imp, op)
}

func (c *authAccountCommand) importRmAction(_ *fisk.ParseContext) error {
	auth, _, acct, err := c.selectAccount(true)
	if err != nil {
		return err
	}

	imp := c.findImport(acct, c.subject)
	if imp == nil {
		return fmt.Errorf("subject %q is not imported", c.subject)
	}

	if !c.force {
		ok, err := askConfirmation(fmt.Sprintf("Really remove the %s import", imp.LocalSubject()), false)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}
	}

	switch imp.(type) {
	case ab.StreamImport:
		_, err = acct.Imports().Streams().Delete(imp.Subject())
		fmt.Printf("Removing stream Import for local Subject %q imported from Account %q\n", imp.LocalSubject(), imp.Account())
	case ab.ServiceImport:
		_, err = acct.Imports().Services().Delete(imp.Subject())
		fmt.Printf("Removing service Import for local subject %q imported from Account %q\n", imp.LocalSubject(), imp.Account())
	}
	if err != nil {
		return err
	}

	return auth.Commit()
}

func (c *authAccountCommand) importSubjects(imports ab.Imports) []string {
	var known []string
	for _, exp := range imports.Services().List() {
		known = append(known, exp.LocalSubject())
	}
	for _, exp := range imports.Streams().List() {
		known = append(known, exp.LocalSubject())
	}

	sort.Strings(known)

	return known
}

func (c *authAccountCommand) fShowImport(w io.Writer, exp ab.Import, op ab.Operator) error {
	out, err := c.showImport(exp, op)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, out)
	return err
}

func (c *authAccountCommand) showImport(imp ab.Import, op ab.Operator) (string, error) {
	cols := newColumns("Import info for import %q importing %q", imp.Name(), imp.LocalSubject())

	_, isStream := imp.(ab.StreamImport)

	src := imp.Account()
	srcAcct, err := op.Accounts().Get(imp.Account())
	if err == nil && srcAcct != nil {
		src = fmt.Sprintf("%s (%s)", srcAcct.Name(), srcAcct.Subject())
	}
	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", imp.Name())
	cols.AddRow("From Account", src)
	cols.AddRow("Local Subject", imp.LocalSubject())
	if isStream {
		cols.AddRow("Kind", "Stream")
	} else {
		cols.AddRow("Kind", "Service")
	}
	cols.AddRow("Remote Subject", imp.Subject())
	cols.AddRow("Sharing Connection Info", imp.IsShareConnectionInfo())

	strImport, ok := imp.(ab.StreamImport)
	if ok {
		cols.AddRow("Allows Message Tracing", strImport.AllowTracing())
	}

	return cols.Render()
}
