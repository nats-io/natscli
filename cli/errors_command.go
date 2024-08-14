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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/choria-io/fisk"
	"github.com/fatih/color"
	"github.com/nats-io/jsm.go/schemas"
	"github.com/nats-io/nats-server/v2/server"
)

type errCmd struct {
	match   string
	code    uint16
	file    string
	sort    string
	reverse bool
}

func configureErrCommand(app commandHost) {
	c := &errCmd{}
	cmd := app.Command("errors", "Error code documentation").Alias("err").Alias("error")
	cmd.Flag("errors", "The errors.json file to use as input").PlaceHolder("FILE").ExistingFileVar(&c.file)
	addCheat("errors", cmd)

	ls := cmd.Command("ls", "List all known error codes").Alias("list").Action(c.listAction)
	ls.Arg("match", "Regular expression match to limit the displayed results").StringVar(&c.match)
	ls.Arg("sort", "Sorts by a specific field (code, http, description, d, desc)").Default("code").EnumVar(&c.sort, "code", "http", "description", "descr", "d")
	ls.Flag("reverse", "Reverse sort").Short('R').BoolVar(&c.reverse)

	lookup := cmd.Command("lookup", "Looks up an error by it's code").Alias("find").Alias("get").Alias("l").Alias("view").Alias("show").Action(c.lookupAction)
	lookup.Arg("code", "The code to retrieve").Required().Uint16Var(&c.code)

	edit := cmd.Command("edit", "Edit or add a error code using your EDITOR").Alias("vi").Alias("add").Alias("new").Action(c.editAction)
	edit.Arg("file", "The file to edit").Required().ExistingFileVar(&c.file)
	edit.Arg("code", "The code to edit").Uint16Var(&c.code)

	validate := cmd.Command("validate", "Validates the validity of the errors definition").Action(c.validateAction)
	validate.Arg("file", "The file to validate").ExistingFileVar(&c.file)
}

func init() {
	registerCommand("errors", 6, configureErrCommand)
}

func (c *errCmd) validateAction(_ *fisk.ParseContext) error {
	if c.file == "" {
		return fmt.Errorf("errors file is required")
	}

	errs, err := c.loadErrors(nil)
	if err != nil {
		return err
	}

	failed := false
	for _, v := range errs {
		if err := c.validateErr(v); err != nil {
			fmt.Printf("%d failed: %s\n", v.ErrCode, err)
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("%s is invalid", c.file)
	}

	fmt.Printf("Validated %d errors\n", len(errs))
	return nil
}

func (c *errCmd) listAction(_ *fisk.ParseContext) error {
	re := regexp.MustCompile(".")
	if c.match != "" {
		re = regexp.MustCompile(strings.ToLower(c.match))
	}

	matched, err := c.loadErrors(re)
	if err != nil {
		return err
	}

	sort.Slice(matched, func(i, j int) bool {
		switch c.sort {
		case "code":
			return c.boolReverse(matched[i].ErrCode < matched[j].ErrCode)
		case "http":
			return c.boolReverse(matched[i].Code < matched[j].Code)
		default:
			return c.boolReverse(matched[i].Description < matched[j].Description)
		}
	})

	table := newTableWriter("NATS Errors")
	table.AddHeaders("NATS Code", "HTTP Error Code", "Description", "Comment", "Go Constant")
	for _, v := range matched {
		table.AddRow(v.ErrCode, v.Code, v.Description, v.Comment, v.Constant)
	}
	fmt.Print(table.Render())
	return nil
}

func (c *errCmd) editAction(pc *fisk.ParseContext) error {
	if os.Getenv("EDITOR") == "" {
		return fmt.Errorf("EDITOR variable is not set")
	}

	errs, err := c.loadErrors(nil)
	if err != nil {
		return err
	}

	var found *server.ErrorsData
	var idx = -1
	var highest uint16

	for i, v := range errs {
		if highest < v.ErrCode {
			highest = v.ErrCode
		}

		if v.ErrCode == c.code {
			found = v
			idx = i
		}
	}

	if found == nil {
		found = &server.ErrorsData{ErrCode: highest + 1}
	}

	fj, err := json.MarshalIndent(found, "", "  ")
	if err != nil {
		return err
	}
	tfile, err := os.CreateTemp("", "*.json")
	if err != nil {
		return err
	}

	tfile.Write(fj)
	tfile.Close()

	for {
		cmd := exec.Command(os.Getenv("EDITOR"), tfile.Name())
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err = cmd.Run()
		if err != nil {
			return fmt.Errorf("could not edit error: %s", err)
		}

		eb, err := os.ReadFile(tfile.Name())
		if err != nil {
			return fmt.Errorf("could not read tempoary file: %s", err)
		}

		found = &server.ErrorsData{}
		err = json.Unmarshal(eb, found)
		if err != nil {
			return err
		}

		err = c.validateErr(found)
		if err != nil {
			fmt.Printf("Invalid error specification: %s\n", err)
			ok, _ := askConfirmation("Retry edit", true)
			if !ok {
				return fmt.Errorf("aborted")
			}
			continue
		}

		break
	}

	if idx == -1 {
		errs = append(errs, found)
	} else {
		errs[idx] = found
	}

	ej, err := json.MarshalIndent(errs, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(c.file, ej, 0600)
	if err != nil {
		return err
	}

	return c.validateAction(pc)
}

func (c *errCmd) lookupAction(_ *fisk.ParseContext) error {
	errs, err := c.loadErrors(nil)
	if err != nil {
		return err
	}

	boldf := color.New(color.Bold).SprintfFunc()

	for _, v := range errs {
		if v.ErrCode == c.code {
			cols := newColumns(fmt.Sprintf("NATS Error Code: %s", boldf("%d", v.ErrCode)))
			cols.AddRow("Description", v.Description)
			cols.AddRowIfNotEmpty("Comment", v.Comment)
			cols.AddRow("HTTP Code", v.Code)
			cols.AddRow("Go Index Constant", v.Constant)
			cols.AddRowIfNotEmpty("Help URL", v.URL)

			cols.Println()
			if v.Help != "" {
				cols.Println(v.Help)
			} else {
				cols.Println("No further information available")
			}
			cols.Frender(os.Stdout)

			return nil
		}
	}

	return fmt.Errorf("no error matches code %d", c.code)
}

func (c *errCmd) loadErrors(re *regexp.Regexp) ([]*server.ErrorsData, error) {
	var (
		ej  []byte
		err error
	)

	if c.file != "" {
		ej, err = os.ReadFile(c.file)
	} else {
		ej, err = schemas.Load("server/errors.json")
	}
	if err != nil {
		return nil, err
	}

	var errs []*server.ErrorsData
	err = json.Unmarshal(ej, &errs)
	if err != nil {
		return nil, err
	}

	if re != nil {
		var matched []*server.ErrorsData
		for _, v := range errs {
			if c.isErrMatch(v, re) {
				matched = append(matched, v)
			}
		}

		if len(matched) == 0 {
			return nil, fmt.Errorf("no errors found matching regular expression %s", re.String())
		}

		return matched, nil
	}

	return errs, nil
}

func (c *errCmd) validateErr(err *server.ErrorsData) error {
	var errs []string

	if single, _ := regexp.MatchString("^{[^}]+}$", err.Description); single && err.Comment == "" {
		errs = append(errs, "token only descriptions requires a comment")
	}
	if err.ErrCode < 1000 {
		errs = append(errs, "error codes may not be below 10000")
	}
	if err.Code == 0 {
		errs = append(errs, "code is require")
	}
	if err.Constant == "" {
		errs = append(errs, "constant is required")
	}
	if err.Description == "" {
		errs = append(errs, "description is required")
	}
	if strings.Count(err.Description, "{") != strings.Count(err.Description, "}") {
		errs = append(errs, "unmatched number of { and }")
	}
	if strings.Contains(err.Description, "{") && !strings.HasSuffix(err.Constant, "F") {
		errs = append(errs, "constants for errors with tokens must end in F like JSStreamMsgDeleteFailedF")
	}

	if len(errs) > 0 {
		return errors.New(f(errs))
	}

	return nil
}

func (c *errCmd) isErrMatch(err *server.ErrorsData, re *regexp.Regexp) bool {
	return re.MatchString(strings.ToLower(err.Description)) ||
		re.MatchString(strconv.Itoa(err.Code)) ||
		re.MatchString(strconv.Itoa(int(err.ErrCode))) ||
		re.MatchString(strings.ToLower(err.Help)) ||
		re.MatchString(strings.ToLower(err.Constant)) ||
		re.MatchString(strings.ToLower(err.Comment))
}

func (c *errCmd) boolReverse(v bool) bool {
	if c.reverse {
		return !v
	}

	return v
}
