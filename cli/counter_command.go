// Copyright 2025 The NATS Authors
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
	"context"
	"fmt"
	"github.com/choria-io/fisk"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/natscli/columns"
	iu "github.com/nats-io/natscli/internal/util"
	"github.com/synadia-io/orbit.go/counters"
	"math/big"
	"os"
)

type counterCmd struct {
	subject string
	value   string
	stream  string
	json    bool
}

func configureCounterCommand(app commandHost) {
	c := counterCmd{}

	ctr := app.Command("counter", "Access distributed Counters").Alias("ctr")
	ctr.HelpLong("This is an experimental command and will undergo changes in later versions")

	get := ctr.Command("get", "Gets the current value for a counter").Alias("g").Action(c.getAction)
	get.Arg("subject", "Subject to get counter for").Required().StringVar(&c.subject)
	get.Flag("stream", "The stream name to fetch the value from").StringVar(&c.stream)

	view := ctr.Command("view", "View the full counter metadata").Alias("v").Action(c.viewAction)
	view.Arg("subject", "Subject to get counter for").Required().StringVar(&c.subject)
	view.Flag("stream", "The stream name to fetch the value from").StringVar(&c.stream)

	incr := ctr.Command("increment", "Increment the value of a counter").Alias("incr").Alias("i").Action(c.incrAction)
	incr.Arg("subject", "Subject to get counter for").Required().StringVar(&c.subject)
	incr.Arg("value", "The value to increment").StringVar(&c.value)
	incr.Flag("stream", "The stream name to fetch the value from").StringVar(&c.stream)

	ls := ctr.Command("list", "List Counters in a stream").Alias("l").Alias("ls").Action(c.lsAction)
	ls.Arg("subject", "Subject pattern to get counters for").StringVar(&c.subject)
	ls.Flag("stream", "The stream name to fetch the value from").StringVar(&c.stream)
	ls.Flag("json", "Produce JSON output").UnNegatableBoolVar(&c.json)
}

func init() {
	registerCommand("counter", 5, configureCounterCommand)
}

func (c *counterCmd) lsAction(_ *fisk.ParseContext) error {
	if c.stream == "" && c.subject == "" {
		return fmt.Errorf("no stream or subject specified")
	}

	stream, _, err := c.getCounterManager()
	if err != nil {
		return err
	}

	if c.subject == "" {
		c.subject = ">"
	}

	nfo, err := stream.Info(context.Background(), jetstream.WithSubjectFilter(c.subject))
	if err != nil {
		return err
	}

	if len(nfo.State.Subjects) == 0 {
		return fmt.Errorf("no counters found")
	}

	var subjects []string
	for k := range nfo.State.Subjects {
		subjects = append(subjects, k)
	}

	if c.json {
		return iu.PrintJSON(subjects)
	}

	for _, subject := range subjects {
		fmt.Println(subject)
	}

	return nil
}

func (c *counterCmd) getAction(_ *fisk.ParseContext) error {
	_, ctr, err := c.getCounterManager()
	if err != nil {
		return err
	}

	val, err := ctr.Load(context.Background(), c.subject)
	if err != nil {
		return err
	}

	fmt.Println(val.String())

	return nil
}

func (c *counterCmd) incrAction(_ *fisk.ParseContext) error {
	_, ctr, err := c.getCounterManager()
	if err != nil {
		return err
	}

	v := &big.Int{}
	_, ok := v.SetString(c.value, 10)
	if !ok {
		return fmt.Errorf("invalid value")
	}

	val, err := ctr.Add(context.Background(), c.subject, v)
	if err != nil {
		return err
	}

	fmt.Println(val.String())

	return nil
}

func (c *counterCmd) viewAction(_ *fisk.ParseContext) error {
	_, ctr, err := c.getCounterManager()
	if err != nil {
		return err
	}

	val, err := ctr.Get(context.Background(), c.subject)
	if err != nil {
		return err
	}

	cols := columns.Newf("Counter %v", c.subject)

	cols.AddRow("Value", val.Value)
	cols.AddRow("Increment", val.Incr)
	cols.AddRow("Subject", val.Subject)

	if len(val.Sources) > 0 {
		cols.AddSectionTitle("Sources")
		for s, v := range val.Sources {
			vals := map[string]string{}
			for k, incr := range v {
				vals[k] = incr.String()
			}

			cols.AddMapStringsAsValue(s, vals)
		}
	} else {
		cols.Println("No source information found")
	}

	return cols.Frender(os.Stdout)
}

func (c *counterCmd) getCounterManager() (jetstream.Stream, counters.Counter, error) {
	nc, _, err := prepareHelper("", natsOpts()...)
	if err != nil {
		return nil, nil, err
	}

	js, err := newJetStreamWithOptions(nc, opts())
	if err != nil {
		return nil, nil, err
	}

	name := c.stream
	if name == "" {
		name, err = js.StreamNameBySubject(context.Background(), c.subject)
		if err != nil {
			return nil, nil, err
		}
	}

	stream, err := js.Stream(context.Background(), name)
	if err != nil {
		return nil, nil, err
	}

	cfg := stream.CachedInfo().Config

	if !(cfg.AllowMsgCounter && cfg.AllowDirect) {
		return nil, nil, fmt.Errorf("%q is not a valid counter stream", name)
	}

	ctr, err := counters.NewCounterFromStream(js, stream)
	if err != nil {
		return nil, nil, err
	}

	return stream, ctr, nil
}
