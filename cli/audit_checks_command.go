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

	if c.json {
		return iu.PrintJSON(collection.Checks())
	}

	tbl := iu.NewTableWriter(opts(), "Audit Checks")
	tbl.AddHeaders("Code", "Description", "Configuration")

	for _, check := range collection.Checks() {
		var cfgKeys []string
		for _, cfg := range check.Configuration {
			switch cfg.Unit {
			case audit.PercentageUnit:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s%%)", cfg.Key, f(int(cfg.Default*100))))
			case audit.IntUnit, audit.UIntUnit:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s)", cfg.Key, f(cfg.Default)))
			default:
				cfgKeys = append(cfgKeys, fmt.Sprintf("%s (%s)", cfg.Key, f(cfg.Default)))
			}
		}
		sort.Strings(cfgKeys)

		tbl.AddRow(check.Code, check.Description, strings.Join(cfgKeys, ", "))
	}

	fmt.Println(tbl.Render())

	return nil
}
