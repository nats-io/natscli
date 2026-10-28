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
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/jsm.go/api"
	gatherer "github.com/nats-io/jsm.go/audit/gather"

	"github.com/choria-io/fisk"
)

type auditGatherCmd struct {
	progress     bool
	config       *gatherer.Configuration
	configFile   string
	writeConfig  string
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
	subszPageSizeIsSet    bool
}

type auditGatherConfigFile struct {
	Cluster          *string                  `json:"cluster"`
	ServerEndpoints  *bool                    `json:"server_endpoints"`
	AccountEndpoints *bool                    `json:"account_endpoints"`
	Streams          *bool                    `json:"streams"`
	Consumers        *bool                    `json:"consumers"`
	Details          *bool                    `json:"details"`
	SubszPageSize    *int                     `json:"subsz_page_size"`
	ServerProfiles   *auditGatherProfilesFile `json:"server_profiles"`
}

type auditGatherProfilesFile struct {
	Enabled     *bool                    `json:"enabled"`
	CPUDuration *string                  `json:"cpu_duration"`
	Profiles    []gatherer.ServerProfile `json:"profiles"`
}

func (p *auditGatherProfilesFile) validate(durationName string) (*time.Duration, error) {
	for _, profile := range p.Profiles {
		if !slices.Contains(gatherer.ServerProfiles, profile) {
			return nil, fmt.Errorf("profiles: unknown server profile %q, valid profiles are %v", profile, gatherer.ServerProfiles)
		}
	}

	if p.CPUDuration == nil {
		return nil, nil
	}

	d, err := fisk.ParseDuration(*p.CPUDuration)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", durationName, err)
	}
	if d <= 0 || d > 15*time.Second {
		return nil, fmt.Errorf("%s: must be more than 0s and at most 15s, got %s", durationName, d)
	}

	return &d, nil
}

func serverProfiles(names []string) []gatherer.ServerProfile {
	var profiles []gatherer.ServerProfile
	for _, name := range names {
		profiles = append(profiles, gatherer.ServerProfile(name))
	}

	return profiles
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
	gather.Flag("config", "JSON file to read gather settings from, create one with --write-config").Envar("NATS_AUDIT_GATHER_CONFIG").PlaceHolder("FILE").ExistingFileVar(&c.configFile)
	gather.Flag("write-config", "Save gather settings to a file instead of gathering").PlaceHolder("FILE").StringVar(&c.writeConfig)
	gather.Flag("output", "output file path of generated archive").Short('o').StringVar(&c.config.TargetPath)
	gather.Flag("progress", "Display progress messages during gathering").Default("true").BoolVar(&c.progress)
	gather.Flag("cluster", "Limit audit to servers from the named cluster").PlaceHolder("CLUSTER").Default("").IsSetByUser(&c.clusterIsSet).StringVar(&c.config.ClusterFilter)
	gather.Flag("server-endpoints", "Capture monitoring endpoints for each server").Default("true").IsSetByUser(&c.serverEndpointsIsSet).BoolVar(&c.config.Include.ServerEndpoints)
	gather.Flag("server-profiles", "Capture profiles for each server").Default("true").IsSetByUser(&c.serverProfilesIsSet).BoolVar(&c.config.Include.ServerProfiles)
	gather.Flag("profile", fmt.Sprintf("Capture only these server profiles (%s)", strings.Join(profileNames, ", "))).PlaceHolder("NAME").EnumsVar(&c.profiles, profileNames...)
	gather.Flag("skip-profile", "Capture the selected server profiles except these").PlaceHolder("NAME").EnumsVar(&c.skipProfiles, profileNames...)
	gather.Flag("cpu-profile-duration", "Duration of the CPU profile captured from each server (max 15s)").Default("5s").IsSetByUser(&c.cpuDurationIsSet).DurationVar(&c.config.CPUProfileDuration)
	gather.Flag("account-endpoints", "Capture monitoring endpoints for each account").Default("true").IsSetByUser(&c.accountEndpointsIsSet).BoolVar(&c.config.Include.AccountEndpoints)
	gather.Flag("streams", "Capture state of each stream").Default("true").IsSetByUser(&c.streamsIsSet).BoolVar(&c.config.Include.Streams)
	gather.Flag("consumers", "Capture state of each stream consumers").Default("true").IsSetByUser(&c.consumersIsSet).BoolVar(&c.config.Include.Consumers)
	gather.Flag("details", "Capture detailed server information from the audit").Default("true").IsSetByUser(&c.detailsIsSet).BoolVar(&c.config.Detailed)
	gather.Flag("subsz-page-size", "Number of subscriptions requested per SUBSZ page").Default("4096").IsSetByUser(&c.subszPageSizeIsSet).IntVar(&c.config.SubszPageSize)
}

func setUnlessFlagged[T any](flagged bool, value *T, target *T) {
	if !flagged && value != nil {
		*target = *value
	}
}

func (c *auditGatherCmd) applyConfigFile() error {
	f, err := os.ReadFile(c.configFile)
	if err != nil {
		return err
	}

	var cfg auditGatherConfigFile
	err = json.Unmarshal(f, &cfg)
	if err != nil {
		return fmt.Errorf("invalid audit gather configuration %s: %w", c.configFile, err)
	}

	setUnlessFlagged(c.clusterIsSet, cfg.Cluster, &c.config.ClusterFilter)
	setUnlessFlagged(c.serverEndpointsIsSet, cfg.ServerEndpoints, &c.config.Include.ServerEndpoints)
	setUnlessFlagged(c.accountEndpointsIsSet, cfg.AccountEndpoints, &c.config.Include.AccountEndpoints)
	setUnlessFlagged(c.streamsIsSet, cfg.Streams, &c.config.Include.Streams)
	setUnlessFlagged(c.consumersIsSet, cfg.Consumers, &c.config.Include.Consumers)
	setUnlessFlagged(c.detailsIsSet, cfg.Details, &c.config.Detailed)
	setUnlessFlagged(c.subszPageSizeIsSet, cfg.SubszPageSize, &c.config.SubszPageSize)

	if cfg.ServerProfiles != nil {
		cpuDuration, err := cfg.ServerProfiles.validate("cpu_duration")
		if err != nil {
			return fmt.Errorf("invalid audit gather configuration %s: %w", c.configFile, err)
		}

		setUnlessFlagged(c.serverProfilesIsSet, cfg.ServerProfiles.Enabled, &c.config.Include.ServerProfiles)
		setUnlessFlagged(c.cpuDurationIsSet, cpuDuration, &c.config.CPUProfileDuration)
		if cfg.ServerProfiles.Profiles != nil {
			c.config.ServerProfileNames = cfg.ServerProfiles.Profiles
		}
	}

	return nil
}

func (c *auditGatherCmd) writeConfigFile() error {
	cpuDuration := c.config.CPUProfileDuration.String()
	cfg := auditGatherConfigFile{
		Cluster:          &c.config.ClusterFilter,
		ServerEndpoints:  &c.config.Include.ServerEndpoints,
		AccountEndpoints: &c.config.Include.AccountEndpoints,
		Streams:          &c.config.Include.Streams,
		Consumers:        &c.config.Include.Consumers,
		Details:          &c.config.Detailed,
		SubszPageSize:    &c.config.SubszPageSize,
		ServerProfiles: &auditGatherProfilesFile{
			Enabled:     &c.config.Include.ServerProfiles,
			CPUDuration: &cpuDuration,
			Profiles:    c.config.ServerProfileNames,
		},
	}

	_, err := cfg.ServerProfiles.validate("--cpu-profile-duration")
	if err != nil {
		return err
	}

	j, _ := json.MarshalIndent(cfg, "", "  ")

	return os.WriteFile(c.writeConfig, append(j, '\n'), 0600)
}

func (c *auditGatherCmd) gather(_ *fisk.ParseContext) error {
	profiles := serverProfiles(c.profiles)
	skipProfiles := serverProfiles(c.skipProfiles)

	if len(profiles) > 0 && len(skipProfiles) > 0 {
		return fmt.Errorf("--profile and --skip-profile cannot be used together")
	}

	if c.configFile != "" {
		err := c.applyConfigFile()
		if err != nil {
			return err
		}
	}

	if c.config.SubszPageSize <= 0 {
		return fmt.Errorf("subsz page size must be more than 0, got %d", c.config.SubszPageSize)
	}

	if !c.config.Include.ServerProfiles && (len(profiles) > 0 || len(skipProfiles) > 0) {
		flag := "--profile"
		if len(skipProfiles) > 0 {
			flag = "--skip-profile"
		}
		if c.serverProfilesIsSet {
			return fmt.Errorf("%s cannot be used with --no-server-profiles", flag)
		}
		return fmt.Errorf("%s cannot be used when %s disables server profiles, add --server-profiles to enable them", flag, c.configFile)
	}

	if len(profiles) > 0 {
		c.config.ServerProfileNames = profiles
	}

	if len(skipProfiles) > 0 {
		c.config.ServerProfileNames = slices.DeleteFunc(c.config.ServerProfileNames, func(profile gatherer.ServerProfile) bool {
			return slices.Contains(skipProfiles, profile)
		})
	}

	if c.config.Include.ServerProfiles && len(c.config.ServerProfileNames) == 0 {
		if len(skipProfiles) == 0 {
			return fmt.Errorf("%s selects no server profiles, choose some with --profile or capture none with --no-server-profiles", c.configFile)
		}
		return fmt.Errorf("no server profiles selected, choose some with --profile or capture none with --no-server-profiles")
	}

	if c.writeConfig != "" {
		return c.writeConfigFile()
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
