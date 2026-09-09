// Copyright 2026 The NATS Authors
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
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/choria-io/fisk"
	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go/backup"
	iu "github.com/nats-io/natscli/internal/util"
)

type backupCmd struct {
	source string
	target string
	json   bool

	subjects            []string
	excludeSubjects     []string
	after               string
	before              string
	firstSeq            uint64
	firstSeqSet         bool
	lastSeq             uint64
	lastSeqSet          bool
	headers             []string
	excludeHeaders      []string
	payloadMatch        []string
	excludePayloadMatch []string
	lastPerSubject      int
	lastPerSubjectSet   bool
	kvCompact           bool
	renumber            bool
	dryRun              bool
	obfuscate           bool
	obfuscationKey      string
	listSubjects        bool
	keyFile             string
	values              []string
}

func configureBackupCommand(app commandHost) {
	c := &backupCmd{}

	bk := app.Command("backup", "Inspect and edit stream backups on disk")
	bk.HelpLong(`These commands work on the directory written by 'nats stream backup'. Only backups taken from NATS Server 2.15 or newer are supported.`)
	addCheat("backup", bk)

	edit := bk.Command("edit", "Creates an edited copy of a stream backup").Action(c.editAction)
	edit.Tag("scope:user", "impact:ro")
	edit.Arg("source", "The directory holding the backup to edit").Required().ExistingDirVar(&c.source)
	edit.Arg("target", "Directory to create the edited backup in").Required().StringVar(&c.target)
	edit.Flag("subject", "Keep messages matching a subject pattern").PlaceHolder("PATTERN").StringsVar(&c.subjects)
	edit.Flag("exclude-subject", "Drop messages matching a subject pattern").PlaceHolder("PATTERN").StringsVar(&c.excludeSubjects)
	edit.Flag("after", "Keep messages stored at or after an RFC3339 time or a duration like 2h").PlaceHolder("TIME").StringVar(&c.after)
	edit.Flag("before", "Keep messages stored before an RFC3339 time or a duration like 2h").PlaceHolder("TIME").StringVar(&c.before)
	edit.Flag("first-seq", "Keep messages with this sequence or higher").PlaceHolder("N").IsSetByUser(&c.firstSeqSet).Uint64Var(&c.firstSeq)
	edit.Flag("last-seq", "Keep messages with this sequence or lower").PlaceHolder("N").IsSetByUser(&c.lastSeqSet).Uint64Var(&c.lastSeq)
	edit.Flag("header", "Keep messages carrying a header, as NAME or NAME:VALUE").PlaceHolder("NAME[:VALUE]").StringsVar(&c.headers)
	edit.Flag("exclude-header", "Drop messages carrying a header").PlaceHolder("NAME").StringsVar(&c.excludeHeaders)
	edit.Flag("payload-match", "Keep messages whose payload matches a regular expression").PlaceHolder("REGEX").StringsVar(&c.payloadMatch)
	edit.Flag("exclude-payload-match", "Drop messages whose payload matches a regular expression").PlaceHolder("REGEX").StringsVar(&c.excludePayloadMatch)
	edit.Flag("last-per-subject", "Keep only the newest N messages of every subject").PlaceHolder("N").IsSetByUser(&c.lastPerSubjectSet).IntVar(&c.lastPerSubject)
	edit.Flag("kv-compact", "Keep the latest revision of each key and drop deleted keys. KV backups only, exclusive with --last-per-subject").UnNegatableBoolVar(&c.kvCompact)
	edit.Flag("renumber", "Number the kept messages from 1 and drop all consumers").UnNegatableBoolVar(&c.renumber)
	edit.Flag("dry-run", "Only shows what the edit would do, does not write the target").UnNegatableBoolVar(&c.dryRun)
	edit.Flag("obfuscate", "Replace names and subjects with keyed hashes and message bodies with zeros of the same length. Keeps counter stream values. Writes the key file beside the target").UnNegatableBoolVar(&c.obfuscate)
	edit.Flag("obfuscation-key", "Reuse the secret and mappings from an earlier key file").PlaceHolder("FILE").StringVar(&c.obfuscationKey)

	info := bk.Command("info", "Stream backup information").Action(c.infoAction)
	info.Tag("scope:user", "impact:ro")
	info.Arg("source", "The directory holding the backup").Required().ExistingDirVar(&c.source)
	info.Flag("subjects", "List every subject in the backup with its message count").UnNegatableBoolVar(&c.listSubjects)
	info.Flag("json", "Produce JSON output").Short('j').UnNegatableBoolVar(&c.json)

	validate := bk.Command("validate", "Validates that a stream backup is complete and restorable").Action(c.validateAction)
	validate.Tag("scope:user", "impact:ro")
	validate.Arg("source", "The directory holding the backup").Required().ExistingDirVar(&c.source)

	lookup := bk.Command("lookup", "Looks up obfuscated values in a key file").Action(c.lookupAction)
	lookup.Tag("scope:user", "impact:ro")
	lookup.Arg("keyfile", "The key file written beside an obfuscated backup").Required().ExistingFileVar(&c.keyFile)
	lookup.Arg("value", "Obfuscated values to look up, subjects are looked up token by token").Required().StringsVar(&c.values)
}

func init() {
	registerCommand("backup", 1, configureBackupCommand)
}

func (c *backupCmd) parseBackupTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	d, err := fisk.ParseDuration(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q: expected an RFC3339 time or a duration like 2h", s)
	}
	return time.Now().Add(-d), nil
}

func (c *backupCmd) editOptions() ([]backup.EditOption, error) {
	var opts []backup.EditOption

	if len(c.subjects) > 0 {
		opts = append(opts, backup.Subjects(c.subjects...))
	}
	if len(c.excludeSubjects) > 0 {
		opts = append(opts, backup.ExcludeSubjects(c.excludeSubjects...))
	}
	if c.after != "" {
		t, err := c.parseBackupTime(c.after)
		if err != nil {
			return nil, err
		}
		opts = append(opts, backup.After(t))
	}
	if c.before != "" {
		t, err := c.parseBackupTime(c.before)
		if err != nil {
			return nil, err
		}
		opts = append(opts, backup.Before(t))
	}
	if c.firstSeqSet {
		opts = append(opts, backup.FirstSeq(c.firstSeq))
	}
	if c.lastSeqSet {
		opts = append(opts, backup.LastSeq(c.lastSeq))
	}
	for _, h := range c.headers {
		name, value, hasValue := strings.Cut(h, ":")
		if hasValue {
			opts = append(opts, backup.HeaderValue(name, value))
		} else {
			opts = append(opts, backup.HeaderPresent(name))
		}
	}
	for _, h := range c.excludeHeaders {
		opts = append(opts, backup.NoHeader(h))
	}
	for _, expr := range c.payloadMatch {
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid payload expression %q: %w", expr, err)
		}
		opts = append(opts, backup.PayloadMatch(re))
	}
	for _, expr := range c.excludePayloadMatch {
		re, err := regexp.Compile(expr)
		if err != nil {
			return nil, fmt.Errorf("invalid payload expression %q: %w", expr, err)
		}
		opts = append(opts, backup.ExcludePayloadMatch(re))
	}
	if c.lastPerSubjectSet {
		opts = append(opts, backup.LastPerSubject(c.lastPerSubject))
	}
	if c.kvCompact {
		opts = append(opts, backup.KVCompact())
	}
	if c.renumber {
		opts = append(opts, backup.Renumber())
	}
	if c.dryRun {
		opts = append(opts, backup.DryRun())
	}
	if c.obfuscate {
		opts = append(opts, backup.Obfuscate())
	}
	if c.obfuscationKey != "" {
		opts = append(opts, backup.ObfuscationKeyFile(c.obfuscationKey))
	}
	opts = append(opts, backup.ToolVersion(fmt.Sprintf("nats %s", Version)))

	return opts, nil
}

func (c *backupCmd) editAction(_ *fisk.ParseContext) error {
	opts, err := c.editOptions()
	if err != nil {
		return err
	}

	res, err := backup.Edit(ctx, c.source, c.target, opts...)
	if err != nil {
		return err
	}

	c.showEditResult(res)

	return nil
}

func (c *backupCmd) showEditResult(res *backup.Result) {
	rep := res.Report

	var table *iu.Table
	if c.dryRun {
		table = iu.NewTableWriter(opts(), "Dry Run, Nothing Was Written")
	} else {
		table = iu.NewTableWriter(opts(), "Backup Edit Report")
	}

	table.AddRow("Stream", res.Config.Name)
	table.AddRow("Target", c.target)
	table.AddSeparator()
	table.AddRow("Source Messages", f(rep.SourceMessages))
	table.AddRow("Source Subjects", f(rep.SourceSubjects))
	table.AddRow("Messages Kept", f(rep.Kept))
	table.AddRow("Subjects Kept", f(rep.KeptSubjects))
	for _, d := range []struct {
		name  string
		count uint64
	}{
		{"Dropped by Sequence Filter", rep.Dropped.Sequence},
		{"Dropped by Time Filter", rep.Dropped.Time},
		{"Dropped by Subject Filter", rep.Dropped.Subject},
		{"Dropped by Header Filter", rep.Dropped.Header},
		{"Dropped by Payload Filter", rep.Dropped.Payload},
		{"Dropped by Last Per Subject", rep.Dropped.LastPerSubject},
		{"Dropped by KV Compaction", rep.Dropped.KVCompact},
	} {
		if d.count > 0 {
			table.AddRow(d.name, f(d.count))
		}
	}
	table.AddRow("Consumers Kept", f(rep.ConsumersKept))
	table.AddRow("Consumers Dropped", f(rep.ConsumersDropped))
	if rep.TombstonesRemovedByContentFilters > 0 {
		table.AddRow("KV Tombstones Removed by Content Filters", f(rep.TombstonesRemovedByContentFilters))
	}
	if c.kvCompact || c.lastPerSubjectSet {
		table.AddRow("Subject State Keys", f(rep.SubjectStateKeys))
		table.AddRow("Subject State Bytes", humanize.IBytes(rep.SubjectStateBytes))
	}

	for _, w := range rep.Warnings {
		table.AddRow("Warning", w)
	}

	table.AddSeparator()
	table.AddRow("Restored Messages", f(res.State.Msgs))
	table.AddRow("Restored Bytes", humanize.IBytes(res.State.Bytes))
	table.AddRow("Restored First Sequence", f(res.State.FirstSeq))
	table.AddRow("Restored Last Sequence", f(res.State.LastSeq))
	table.AddRow("Restored Consumers", f(res.State.Consumers))

	if rep.Obfuscation != nil {
		table.AddSeparator()
		table.AddRow("Obfuscated Tokens", f(rep.Obfuscation.TokensMapped))
		table.AddRow("Message Bodies Padded", f(rep.Obfuscation.BodiesPadded))
		if rep.Obfuscation.KeyFile != "" {
			table.AddRow("Key File", rep.Obfuscation.KeyFile)
		}
	}

	fmt.Println(table.Render())

	if rep.Obfuscation != nil && rep.Obfuscation.KeyFile != "" {
		fmt.Println("The key file maps the obfuscated names back to the originals: share the backup, keep the key file private.")
		fmt.Println()
	}
}

func (c *backupCmd) infoAction(_ *fisk.ParseContext) error {
	var infoOpts []backup.InfoOption
	if c.listSubjects {
		infoOpts = append(infoOpts, backup.WithSubjects())
	}
	nfo, err := backup.Info(c.source, infoOpts...)
	if err != nil {
		return err
	}

	if c.json {
		return iu.PrintJSON(nfo)
	}

	cfg := nfo.Config
	cols := newColumnsf("Information for backup of Stream %s in %s", cfg.Name, c.source)

	cols.AddSectionTitle("Configuration")
	cols.AddRow("Name", cfg.Name)
	cols.AddRowIf("Subjects", cfg.Subjects, len(cfg.Subjects) > 0)
	cols.AddRow("Storage", cfg.Storage.String())
	cols.AddRow("Retention", cfg.Retention.String())
	cols.AddRow("Replicas", cfg.Replicas)
	cols.AddRowUnlimited("Maximum Messages", cfg.MaxMsgs, -1)
	cols.AddRowUnlimitedIf("Maximum Per Subject", cfg.MaxMsgsPer, cfg.MaxMsgsPer <= 0)
	cols.AddRowUnlimitedIf("Maximum Bytes", humanize.IBytes(uint64(max(cfg.MaxBytes, 0))), cfg.MaxBytes == -1)
	cols.AddRowUnlimitedIf("Maximum Age", cfg.MaxAge, cfg.MaxAge <= 0)
	cols.AddRowUnlimitedIf("Maximum Message Size", humanize.IBytes(uint64(max(int64(cfg.MaxMsgSize), 0))), cfg.MaxMsgSize == -1)
	cols.AddRowUnlimited("Maximum Consumers", int64(cfg.MaxConsumers), -1)

	cols.AddSectionTitle("Consumers")
	cols.AddRow("Count", len(nfo.Consumers))
	cols.AddRowIf("Names", nfo.Consumers, len(nfo.Consumers) > 0)

	cols.AddSectionTitle("Messages")
	cols.AddRow("Messages", nfo.Messages)
	cols.AddRow("Subjects", nfo.NumSubjects)
	cols.AddRow("Bytes", humanize.IBytes(nfo.Bytes))
	if nfo.Messages > 0 {
		cols.AddRowf("First Sequence", "%s @ %s", f(nfo.FirstSeq), f(nfo.FirstTime))
		cols.AddRowf("Last Sequence", "%s @ %s", f(nfo.LastSeq), f(nfo.LastTime))
	}

	if nfo.DeclaredCountsAdvisory {
		cols.AddSectionTitle("Declared State (message and byte counts are advisory)")
	} else {
		cols.AddSectionTitle("Declared State")
	}
	cols.AddRow("Messages", nfo.Declared.Msgs)
	cols.AddRow("Bytes", humanize.IBytes(nfo.Declared.Bytes))
	cols.AddRow("First Sequence", nfo.Declared.FirstSeq)
	cols.AddRow("Last Sequence", nfo.Declared.LastSeq)
	cols.AddRow("Consumers", nfo.Declared.Consumers)

	if nfo.Edit != nil {
		cols.AddSectionTitle("Edit")
		cols.AddRowIf("Tool Version", nfo.Edit.Version, nfo.Edit.Version != "")
		if len(nfo.Edit.Options) > 0 {
			cols.AddRow("Options", nfo.Edit.Options)
		} else {
			cols.AddRow("Options", "none")
		}
		cols.AddRow("Source Digest", nfo.Edit.SourceDigest)
		cols.AddRow("Obfuscated", nfo.Edit.Obfuscated)
	}

	cols.Frender(os.Stdout)

	if c.listSubjects {
		fmt.Println()
		c.showSubjects(nfo)
	}

	return nil
}

func (c *backupCmd) showSubjects(nfo *backup.InfoReport) {
	names := make([]string, 0, len(nfo.Subjects))
	for s := range nfo.Subjects {
		names = append(names, s)
	}
	sort.Strings(names)

	fmt.Println(subjectsTable(fmt.Sprintf("%d Subjects in backup of stream %s", len(names), nfo.Config.Name), names, nfo.Subjects).Render())
}

func (c *backupCmd) validateAction(_ *fisk.ParseContext) error {
	rep, err := backup.Verify(c.source)
	if err != nil {
		return err
	}

	seqs := "no messages"
	if rep.Messages > 0 {
		seqs = fmt.Sprintf("sequences %s to %s", f(rep.FirstSeq), f(rep.LastSeq))
	}
	fmt.Printf("OK: %s entries, %s consumers, %s messages, %s subjects, %s\n", f(rep.Entries), f(rep.Consumers), f(rep.Messages), f(rep.NumSubjects), seqs)

	return nil
}

func (c *backupCmd) lookupAction(_ *fisk.ParseContext) error {
	kf, err := backup.LoadKeyFile(c.keyFile)
	if err != nil {
		return err
	}

	for _, v := range c.values {
		fmt.Println(kf.Reveal(v))
	}

	return nil
}
