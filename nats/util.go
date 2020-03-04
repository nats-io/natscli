// Copyright 2020 The NATS Authors
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
	"log"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/AlecAivazis/survey/v2"
	"github.com/nats-io/nats.go"

	"github.com/nats-io/jsm.go"
)

func selectConsumer(stream string, consumer string) (string, error) {
	if consumer != "" {
		known, err := jsm.IsKnownConsumer(stream, consumer)
		if err != nil {
			return "", err
		}

		if known {
			return consumer, nil
		}
	}

	consumers, err := jsm.ConsumerNames(stream)
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
		known, err := jsm.IsKnownStreamTemplate(template)
		if err != nil {
			return "", err
		}

		if known {
			return template, nil
		}
	}

	templates, err := jsm.StreamTemplateNames()
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
		known, err := jsm.IsKnownStream(stream)
		if err != nil {
			return "", err
		}

		if known {
			return stream, nil
		}
	}

	streams, err := jsm.StreamNames()
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
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts := []nats.Option{
		nats.Name("NATS CLI"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(reconnectDelay),
		nats.MaxReconnects(int(totalWait / reconnectDelay)),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("Disconnected due to: %s, will attempt reconnects for %.0fm", err.Error(), totalWait.Minutes())
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("Reconnected [%s]", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			err := nc.LastError()
			if err != nil {
				log.Fatalf("Exiting: %v", nc.LastError())
			}
		}),
	}

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
		jsm.SetTimeout(timeout)
	}

	jsm.SetConnection(nc)

	return nc, err
}

func humanizeTime(t time.Time) string {
	d := time.Since(t)
	// Just use total seconds for uptime, and display days / years
	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}

	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}

	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}

	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}

	return fmt.Sprintf("%ds", tsecs)
}
