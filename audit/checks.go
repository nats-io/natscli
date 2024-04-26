package audit

import (
	"fmt"

	"github.com/nats-io/natscli/archive"
)

type Check struct {
	Name        string
	Description string
	fun         checkFunc
}

type Outcome int
type checkFunc func(reader *archive.Reader) (Outcome, error)

const (
	Pass           Outcome = iota
	PassWithIssues Outcome = iota
	Fail           Outcome = iota
	Skipped        Outcome = iota
)

func (o Outcome) string() string {
	switch o {
	case Fail:
		return "FAIL"
	case Pass:
		return "PASS"
	case PassWithIssues:
		return "WARN"
	case Skipped:
		return "SKIP"
	default:
		panic(fmt.Sprintf("Uknown outcome code: %d", o))
	}
}

func (c *Check) Run(reader *archive.Reader) (Outcome, error) {
	outcome, err := c.fun(reader)
	if err != nil {
		return Skipped, fmt.Errorf("check %s failed: %w", c.Name, err)
	}
	return outcome, nil
}

func GetDefaultChecks() []Check {

	// Defaults
	const (
		cpuThreshold = 0.9 // Warn using >90% CPU
	)

	return []Check{
		{
			Name:        "Server version",
			Description: "Verify that the entire fleet is running the same nats-server version",
			fun:         checkServerVersion,
		},
		{
			Name:        "CPU Usage",
			Description: "Verify that aggregate CPU usage for each server is below a given threshold",
			fun:         makeCheckCPUUsage(cpuThreshold),
		},
	}
}

// This is an example of non-parametrized check
func checkServerVersion(reader *archive.Reader) (Outcome, error) {
	// TODO for each server VARZ in archive, save version
	//   if de-duplicated list of versions > 1, fail
	return Pass, nil
}

// This is an example of a parametrized check
func makeCheckCPUUsage(threshold float64) checkFunc {
	return func(reader *archive.Reader) (Outcome, error) {
		// TODO for each server VARZ in archive, ...
		//   if usage > threshold ...
		return Pass, nil
	}
}
