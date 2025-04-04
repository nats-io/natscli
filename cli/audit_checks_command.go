package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/choria-io/fisk"
	"github.com/nats-io/jsm.go/audit"
	iu "github.com/nats-io/natscli/internal/util"
)

type auditChecksCommand struct {
	json bool
}

func configureAuditChecksCommand(app *fisk.CmdClause) {
	c := &auditChecksCommand{}

	checks := app.Command("checks", "List configured audit checks").Alias("ls").Action(c.checksAction)
	checks.Flag("json", "Produce JSON output").UnNegatableBoolVar(&c.json)
}

func (c *auditChecksCommand) checksAction(_ *fisk.ParseContext) error {
	collection, err := audit.NewDefaultCheckCollection()
	if err != nil {
		return err
	}

	var checks []*audit.Check
	collection.EachCheck(func(c *audit.Check) {
		checks = append(checks, c)
	})

	if c.json {
		return iu.PrintJSON(checks)
	}

	tbl := iu.NewTableWriter(opts(), "Audit Checks")
	tbl.AddHeaders("Suite", "Code", "Description", "Configuration")

	for _, check := range checks {
		var cfgKeys []string
		for _, cfg := range check.Configuration {
			switch cfg.Unit {
			case audit.PercentageUnit:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s%%)", cfg.Key, f(int(cfg.Default))))
			case audit.IntUnit, audit.UIntUnit:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s)", cfg.Key, f(cfg.Default)))
			default:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s)", cfg.Key, f(cfg.Default)))
			}
		}
		sort.Strings(cfgKeys)

		tbl.AddRow(check.Suite, check.Code, check.Description, strings.Join(cfgKeys, ", "))
	}

	fmt.Println(tbl.Render())

	return nil
}
