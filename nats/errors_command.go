package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/fatih/color"
	"github.com/nats-io/jsm.go/schemas"
	"github.com/nats-io/nats-server/v2/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

type errCmd struct {
	match   string
	code    uint16
	file    string
	sort    string
	reverse bool
}

func configureErrCommand(app *kingpin.Application) {
	c := &errCmd{}
	cmd := app.Command("errors", "Error code documentation").Alias("err").Alias("error")
	cmd.Flag("errors", "The errors.json file to use as input").PlaceHolder("FILE").ExistingFileVar(&c.file)

	list := cmd.Command("list", "List all known error codes").Alias("ls").Action(c.listAction)
	list.Arg("match", "Regular expression match to limit the displayed results").StringVar(&c.match)
	list.Arg("sort", "Sorts by a specific field (code, http, description, d, desc)").Default("code").EnumVar(&c.sort, "code", "http", "description", "descr", "d")
	list.Flag("reverse", "Reverse sort").Short('R').BoolVar(&c.reverse)

	lookup := cmd.Command("lookup", "Looks up an error by it's code").Alias("find").Alias("get").Alias("l").Alias("view").Alias("show").Action(c.lookupAction)
	lookup.Arg("code", "The code to retrieve").Required().Uint16Var(&c.code)

	edit := cmd.Command("edit", "Edit or add a error code using your EDITOR").Alias("vi").Alias("add").Alias("new").Action(c.editAction)
	edit.Arg("file", "The file to edit").Required().ExistingFileVar(&c.file)
	edit.Arg("code", "The code to edit").Uint16Var(&c.code)

	validate := cmd.Command("validate", "Validates the validity of the errors definition").Action(c.validateAction)
	validate.Arg("file", "The file to validate").ExistingFileVar(&c.file)

	cheats["errors"] = `# To look up information for error code 1000
nats errors lookup 1000

# To list all errors mentioning stream using regular expression matches
nats errors list stream

# As a NATS Server developer edit an existing code in errors.json
nats errors edit errors.json 10013

# As a NATS Server developer add a new code to the errors.json, auto picking a code 
nats errors add errors.json 
`
}

func (c *errCmd) validateAction(_ *kingpin.ParseContext) error {
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

func (c *errCmd) listAction(_ *kingpin.ParseContext) error {
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

func (c *errCmd) editAction(pc *kingpin.ParseContext) error {
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
	tfile, err := ioutil.TempFile("", "")
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

		eb, err := ioutil.ReadFile(tfile.Name())
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

	err = os.WriteFile(c.file, ej, 0644)
	if err != nil {
		return err
	}

	return c.validateAction(pc)
}

func (c *errCmd) lookupAction(_ *kingpin.ParseContext) error {
	errs, err := c.loadErrors(nil)
	if err != nil {
		return err
	}

	boldf := color.New(color.Bold).SprintfFunc()

	for _, v := range errs {
		if v.ErrCode == c.code {
			fmt.Printf("NATS Error Code: %s\n\n", boldf("%d", v.ErrCode))
			fmt.Printf("        Description: %s\n", v.Description)
			if v.Comment != "" {
				fmt.Printf("            Comment: %s\n", v.Comment)
			}
			fmt.Printf("          HTTP Code: %d\n", v.Code)
			fmt.Printf("  Go Index Constant: %s\n", v.Constant)
			if v.URL != "" {
				fmt.Printf("           Help URL: %s\n", v.URL)
			}
			if v.Help != "" {
				fmt.Printf("\n%s\n", v.Help)
			} else {
				fmt.Printf("\nNo further information available\n")
			}

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
		ej, err = ioutil.ReadFile(c.file)
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
		return fmt.Errorf(strings.Join(errs, ", "))
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
