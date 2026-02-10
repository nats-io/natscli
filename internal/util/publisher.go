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

package util

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/options"
)

const (
	sendOnEOF     = "eof"
	sendOnNewline = "newline"
)

// Publisher manages stdin reading, progress tracking, and template expansion for pub and req commands
// and calls their respective execution callbacks
type Publisher struct {
	UseStdin bool
	Tracker  *progress.Tracker

	reader      *bufio.Reader
	progressBar progress.Writer
	templates   bool
	sendOn      string
	opts        *options.Options
}

// PublisherConfig contains options for creating a new Publisher
type PublisherConfig struct {
	BodyIsSet  bool
	ForceStdin bool
	Count      int
	Raw        bool
	Templates  bool
	Opts       *options.Options
}

// NewPublisher sets up stdin and the progress bar
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	p := &Publisher{
		templates: cfg.Templates,
		sendOn:    sendOnEOF,
		opts:      cfg.Opts,
	}

	p.UseStdin = !cfg.BodyIsSet && (IsTerminal() || cfg.ForceStdin)
	if p.UseStdin {
		readPipe, writePipe := io.Pipe()
		p.reader = bufio.NewReader(readPipe)
		go func() {
			_, err := io.Copy(writePipe, os.Stdin)
			if err != nil {
				writePipe.CloseWithError(err)
			} else {
				writePipe.Close()
			}
		}()
	}

	if cfg.Count > 20 && !cfg.Raw {
		var err error
		p.progressBar, p.Tracker, err = NewProgress(cfg.Opts, &progress.Tracker{
			Total: int64(cfg.Count),
		})
		if err != nil {
			return nil, err
		}
	}

	return p, nil
}

func (p *Publisher) SetSendOnNewLine() {
	p.sendOn = sendOnNewline
}

func (p *Publisher) IsSendOnEOF() bool {
	return p.sendOn == sendOnEOF
}

func (p *Publisher) IsSendOnNewLine() bool {
	return p.sendOn == sendOnNewline
}

// ReadStdin reads from stdin based on sendOn mode.
func (p *Publisher) ReadStdin() (string, bool, error) {
	if p.reader == nil {
		return "", true, nil
	}

	switch p.sendOn {
	case sendOnEOF:
		body, err := io.ReadAll(p.reader)
		if err != nil {
			return "", false, err
		}
		return string(body), true, nil
	case sendOnNewline:
		var buf bytes.Buffer
		for {
			line, isPrefix, err := p.reader.ReadLine()
			if err != nil {
				body := buf.String()
				if err == io.EOF {
					return body, true, nil
				}
				return body, false, err
			}
			buf.Write(line)
			if !isPrefix {
				return buf.String(), false, nil
			}
		}
	default:
		return "", false, fmt.Errorf("unknown send-on mode: %s", p.sendOn)
	}
}

// ParseTemplates applies template expansion to body and subject if templates are enabled.
func (p *Publisher) ParseTemplates(body, subject string, ctr int) (string, string, error, error) {
	if !p.templates {
		return body, subject, nil, nil
	}

	expandedBody, bodyErr := PubReplyBodyTemplate(body, "", ctr)
	expandedSubj, subjErr := PubReplyBodyTemplate(subject, "", ctr)

	return string(expandedBody), string(expandedSubj), bodyErr, subjErr
}

// PrepareMsg creates a nats.Msg with subject, body, reply subject, and headers
func (p *Publisher) PrepareMsg(subj, replyTo string, body []byte, hdrs []string, seq int) (*nats.Msg, error) {
	msg := nats.NewMsg(subj)
	msg.Reply = replyTo
	msg.Data = body

	err := ParseStringsToMsgHeader(hdrs, seq, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (p *Publisher) StopProgress() {
	if p.progressBar != nil {
		p.progressBar.Stop()
		time.Sleep(300 * time.Millisecond)
	}
}

func (p *Publisher) Run(ctx context.Context, callback func() error) error {
	complete := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		defer close(complete)
		errCh <- callback()
	}()

	select {
	case <-ctx.Done():
		if p.reader != nil {
			if rp, ok := any(p.reader).(interface{ Close() error }); ok {
				rp.Close()
			}
		}
		return fmt.Errorf("interrupted")
	case <-complete:
		return <-errCh
	}
}
