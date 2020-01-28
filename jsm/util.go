// Copyright 2019 The NATS Authors
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

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/AlecAivazis/survey/v2"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/jetstream/jsch"
)

func selectConsumer(stream string, consumer string) (string, error) {
	if consumer != "" {
		known, err := jsch.IsKnownConsumer(stream, consumer)
		if err != nil {
			return "", err
		}

		if known {
			return consumer, nil
		}
	}

	consumers, err := jsch.ConsumerNames(stream)
	if err != nil {
		return "", err
	}

	switch len(consumers) {
	case 0:
		return "", fmt.Errorf("no Consumers are defined for Stream %s", stream)
	default:
		c := ""

		err = survey.AskOne(&survey.Select{
			Message: "Select a Consumer",
			Options: consumers,
		}, &c)
		if err != nil {
			return "", err
		}

		return c, nil
	}
}

func selectStreamTemplate(template string) (string, error) {
	if template != "" {
		known, err := jsch.IsKnownStreamTemplate(template)
		if err != nil {
			return "", err
		}

		if known {
			return template, nil
		}
	}

	templates, err := jsch.StreamTemplateNames()
	if err != nil {
		return "", err
	}

	switch len(templates) {
	case 0:
		return "", errors.New("no Streams Templates are defined")
	default:
		s := ""

		err = survey.AskOne(&survey.Select{
			Message: "Select a Stream Template",
			Options: templates,
		}, &s)
		if err != nil {
			return "", err
		}

		return s, nil
	}
}

func selectStream(stream string) (string, error) {
	if stream != "" {
		known, err := jsch.IsKnownStream(stream)
		if err != nil {
			return "", err
		}

		if known {
			return stream, nil
		}
	}

	streams, err := jsch.StreamNames()
	if err != nil {
		return "", err
	}

	switch len(streams) {
	case 0:
		return "", errors.New("no Streams are defined")
	default:
		s := ""

		err = survey.AskOne(&survey.Select{
			Message: "Select a Stream",
			Options: streams,
		}, &s)
		if err != nil {
			return "", err
		}

		return s, nil
	}
}

func printJSON(d interface{}) error {
	j, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}
func parseDurationString(dstr string) (dur time.Duration, err error) {
	dstr = strings.TrimSpace(dstr)

	if len(dstr) <= 0 {
		return dur, nil
	}

	ls := len(dstr)
	di := ls - 1
	unit := dstr[di:]

	switch unit {
	case "w", "W":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*7*24) * time.Hour

	case "d", "D":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24) * time.Hour
	case "M":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24*30) * time.Hour
	case "Y", "y":
		val, err := strconv.ParseFloat(dstr[:di], 32)
		if err != nil {
			return dur, err
		}

		dur = time.Duration(val*24*365) * time.Hour
	case "s", "S", "m", "h", "H":
		dur, err = time.ParseDuration(dstr)
		if err != nil {
			return dur, err
		}

	default:
		return dur, fmt.Errorf("invalid time unit %s", unit)
	}

	return dur, nil
}

func askConfirmation(prompt string, dflt bool) (bool, error) {
	ans := dflt

	err := survey.AskOne(&survey.Confirm{
		Message: prompt,
		Default: dflt,
	}, &ans)

	return ans, err
}

func askOneInt(prompt string, dflt string, help string) (int64, error) {
	val := ""
	err := survey.AskOne(&survey.Input{
		Message: prompt,
		Default: dflt,
		Help:    help,
	}, &val)
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}

	return int64(i), nil
}

func splitString(s string) []string {
	return strings.FieldsFunc(s, func(c rune) bool {
		if unicode.IsSpace(c) {
			return true
		}

		if c == ',' {
			return true
		}

		return false
	})
}

func natsOpts() []nats.Option {
	opts := []nats.Option{nats.Name("JetStream Management CLI"), nats.MaxReconnects(-1)}
	if creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}

	if tlsCert != "" && tlsKey != "" {
		opts = append(opts, nats.ClientCert(tlsCert, tlsKey))
	}

	if tlsCA != "" {
		opts = append(opts, nats.RootCAs(tlsCA))
	}

	return opts
}

func newNatsConn(servers string, opts ...nats.Option) (*nats.Conn, error) {
	return nats.Connect(servers, opts...)
}

func prepareHelper(servers string, opts ...nats.Option) (*nats.Conn, error) {
	nc, err := newNatsConn(servers, opts...)
	if err != nil {
		return nil, err
	}

	if timeout != 0 {
		jsch.Timeout = timeout
	}

	jsch.SetConnection(nc)

	return nc, err
}
