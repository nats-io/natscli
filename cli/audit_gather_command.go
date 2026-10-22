// Copyright 2024-2025 The NATS Authors
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
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/jsm.go/api"
	gatherer "github.com/nats-io/jsm.go/audit/gather"

	"github.com/choria-io/fisk"
	"gopkg.in/yaml.v3"
)

type auditGatherCmd struct {
	progress     bool
	config       *gatherer.Configuration
	configFile   string
	profiles     []string
	skipProfiles []string

	clusterIsSet          bool
	serverEndpointsIsSet  bool
	serverProfilesIsSet   bool
	cpuDurationIsSet      bool
	accountEndpointsIsSet bool
	streamsIsSet          bool
	consumersIsSet        bool
	detailsIsSet          bool
}

type auditGatherConfigFile struct {
	Cluster          *string                  `yaml:"cluster"`
	ServerEndpoints  *bool                    `yaml:"server_endpoints"`
	AccountEndpoints *bool                    `yaml:"account_endpoints"`
	Streams          *bool                    `yaml:"streams"`
	Consumers        *bool                    `yaml:"consumers"`
	Details          *bool                    `yaml:"details"`
	ServerProfiles   *auditGatherProfilesFile `yaml:"server_profiles"`
}

type auditGatherProfilesFile struct {
	Enabled     *bool                    `yaml:"enabled"`
	CPUDuration *time.Duration           `yaml:"cpu_duration"`
	Profiles    []gatherer.ServerProfile `yaml:"profiles"`
}

func configureAuditGatherCommand(app *fisk.CmdClause) {
	c := &auditGatherCmd{
		config: gatherer.NewCaptureConfiguration(),
	}

	var profileNames []string
	for _, profile := range gatherer.ServerProfiles {
		profileNames = append(profileNames, string(profile))
	}

	gather := app.Command("gather", "capture a variety of data from a deployment into an archive file").Alias("capture").Alias("cap").Action(c.gather)
	gather.Tag("scope:system", "impact:ro")
	gather.Flag("config", "YAML file to read gather settings from").Envar("NATS_AUDIT_GATHER_CONFIG").PlaceHolder("FILE").ExistingFileVar(&c.configFile)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.config.TargetPath)
	gather.Flag("progress", "Display progress messages during gathering").Default("true").BoolVar(&c.progress)
	gather.Flag("cluster", "Limit audit to servers from the named cluster").PlaceHolder("CLUSTER").Default("").IsSetByUser(&c.clusterIsSet).StringVar(&c.config.ClusterFilter)
	gather.Flag("server-endpoints", "Capture monitoring endpoints for each server").Default("true").IsSetByUser(&c.serverEndpointsIsSet).BoolVar(&c.config.Include.ServerEndpoints)
	gather.Flag("server-profiles", "Capture profiles for each server").Default("true").IsSetByUser(&c.serverProfilesIsSet).BoolVar(&c.config.Include.ServerProfiles)
	gather.Flag("profile", fmt.Sprintf("Capture only these server profiles (%s)", strings.Join(profileNames, ", "))).PlaceHolder("NAME").EnumsVar(&c.profiles, profileNames...)
	gather.Flag("skip-profile", "Server profiles to leave out of the capture").PlaceHolder("NAME").EnumsVar(&c.skipProfiles, profileNames...)
	gather.Flag("cpu-profile-duration", "Duration of the CPU profile captured from each server (max 15s)").Default("5s").IsSetByUser(&c.cpuDurationIsSet).DurationVar(&c.config.CPUProfileDuration)
	gather.Flag("account-endpoints", "Capture monitoring endpoints for each account").Default("true").IsSetByUser(&c.accountEndpointsIsSet).BoolVar(&c.config.Include.AccountEndpoints)
	gather.Flag("streams", "Capture state of each stream").Default("true").IsSetByUser(&c.streamsIsSet).BoolVar(&c.config.Include.Streams)
	gather.Flag("consumers", "Capture state of each stream consumers").Default("true").IsSetByUser(&c.consumersIsSet).BoolVar(&c.config.Include.Consumers)
	gather.Flag("details", "Capture detailed server information from the audit").Default("true").IsSetByUser(&c.detailsIsSet).BoolVar(&c.config.Detailed)
}

func setUnlessFlagged[T any](flagged bool, value *T, target *T) {
	if !flagged && value != nil {
		*target = *value
	}
}

func (c *auditGatherCmd) applyConfigFile() error {
	f, err := os.Open(c.configFile)
	if err != nil {
		return err
	}
	defer f.Close()

	var cfg auditGatherConfigFile
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	err = dec.Decode(&cfg)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("invalid audit gather configuration %s: %w", c.configFile, err)
	}

	setUnlessFlagged(c.clusterIsSet, cfg.Cluster, &c.config.ClusterFilter)
	setUnlessFlagged(c.serverEndpointsIsSet, cfg.ServerEndpoints, &c.config.Include.ServerEndpoints)
	setUnlessFlagged(c.accountEndpointsIsSet, cfg.AccountEndpoints, &c.config.Include.AccountEndpoints)
	setUnlessFlagged(c.streamsIsSet, cfg.Streams, &c.config.Include.Streams)
	setUnlessFlagged(c.consumersIsSet, cfg.Consumers, &c.config.Include.Consumers)
	setUnlessFlagged(c.detailsIsSet, cfg.Details, &c.config.Detailed)

	if cfg.ServerProfiles != nil {
		setUnlessFlagged(c.serverProfilesIsSet, cfg.ServerProfiles.Enabled, &c.config.Include.ServerProfiles)
		setUnlessFlagged(c.cpuDurationIsSet, cfg.ServerProfiles.CPUDuration, &c.config.CPUProfileDuration)
		if cfg.ServerProfiles.Profiles != nil {
			c.config.ServerProfileNames = cfg.ServerProfiles.Profiles
		}
	}

	return nil
}

func (c *auditGatherCmd) gather(_ *fisk.ParseContext) error {
	if len(c.profiles) > 0 && len(c.skipProfiles) > 0 {
		return fmt.Errorf("--profile and --skip-profile cannot be used together")
	}

	if c.configFile != "" {
		err := c.applyConfigFile()
		if err != nil {
			return err
		}
	}

	if !c.config.Include.ServerProfiles && (len(c.profiles) > 0 || len(c.skipProfiles) > 0) {
		flag := "--profile"
		if len(c.skipProfiles) > 0 {
			flag = "--skip-profile"
		}
		if c.serverProfilesIsSet {
			return fmt.Errorf("%s cannot be used with --no-server-profiles", flag)
		}
		return fmt.Errorf("%s cannot be used when %s disables server profiles, add --server-profiles to enable them", flag, c.configFile)
	}

	if len(c.profiles) > 0 {
		c.config.ServerProfileNames = nil
		for _, name := range c.profiles {
			c.config.ServerProfileNames = append(c.config.ServerProfileNames, gatherer.ServerProfile(name))
		}
	}

	if len(c.skipProfiles) > 0 {
		c.config.ServerProfileNames = slices.DeleteFunc(c.config.ServerProfileNames, func(profile gatherer.ServerProfile) bool {
			return slices.Contains(c.skipProfiles, string(profile))
		})
	}

	nc, err := newNatsConn("", natsOpts()...)
	if err != nil {
		return err
	}
	defer nc.Close()

	c.config.Version = fmt.Sprintf("nats %s", Version)

	switch {
	case opts().Trace:
		c.config.LogLevel = api.TraceLevel
	case c.progress:
		c.config.LogLevel = api.InfoLevel
	default:
		c.config.LogLevel = api.ErrorLevel
	}

	c.config.Timeout = opts().Timeout

	return gatherer.Gather(nc, c.config)
}
