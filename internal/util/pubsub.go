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

	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/natscli/options"
)

const (
	SendOnEOF     = "eof"
	SendOnNewline = "newline"
)

// SetupStdin sets up stdin reading if appropriate.
// Returns the reader and a boolean indicating if stdin is being used.
func SetupStdin(bodyIsSet bool, forceStdin bool) (*bufio.Reader, bool) {
	useStdin := !bodyIsSet && (IsTerminal() || forceStdin)
	if !useStdin {
		return nil, false
	}

	readPipe, writePipe := io.Pipe()
	reader := bufio.NewReader(readPipe)

	go func() {
		_, err := io.Copy(writePipe, os.Stdin)
		if err != nil {
			writePipe.CloseWithError(err)
		} else {
			_ = writePipe.Close()
		}
	}()

	return reader, true
}

// PrepareMsg creates a nats.Msg with subject, body, reply subject, and headers
func PrepareMsg(subj, replyTo string, body []byte, hdrs []string, seq int) (*nats.Msg, error) {
	msg := nats.NewMsg(subj)
	msg.Reply = replyTo
	msg.Data = body

	err := ParseStringsToMsgHeader(hdrs, seq, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// ParseTemplates applies template expansion to body and subject if templates are enabled.
func ParseTemplates(body, subject string, ctr int, enableTemplates bool) (string, string, error, error) {
	if !enableTemplates {
		return body, subject, nil, nil
	}

	expandedBody, bodyErr := PubReplyBodyTemplate(body, "", ctr)
	expandedSubj, subjErr := PubReplyBodyTemplate(subject, "", ctr)

	return string(expandedBody), string(expandedSubj), bodyErr, subjErr
}

// SetupProgressBar creates a progress bar if count > 20 and raw mode is not enabled
func SetupProgressBar(cnt int, raw bool, opts *options.Options) (progress.Writer, *progress.Tracker, error) {
	if cnt <= 20 || raw {
		return nil, nil, nil
	}

	progbar, tracker, err := NewProgress(opts, &progress.Tracker{
		Total: int64(cnt),
	})
	if err != nil {
		return nil, nil, err
	}

	return progbar, tracker, nil
}

// ReadStdin reads from stdin based on sendOn mode.
func ReadStdin(reader *bufio.Reader, sendOn string) (string, bool, error) {
	switch sendOn {
	case SendOnEOF:
		body, err := io.ReadAll(reader)
		if err != nil {
			return "", false, err
		}
		return string(body), true, nil
	case SendOnNewline:
		var buf bytes.Buffer
		for {
			line, isPrefix, err := reader.ReadLine()
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
		return "", false, fmt.Errorf("unknown send-on mode: %s", sendOn)
	}
}

// CleanupOnInterrupt handles the cleanup and waiting logic for pub and req commands
func CleanupOnInterrupt(ctx context.Context, reader *bufio.Reader, complete <-chan struct{}, errCh <-chan error) error {
	select {
	case <-ctx.Done():
		if reader != nil {
			// Close the underlying pipe reader
			if rp, ok := any(reader).(interface{ Close() error }); ok {
				rp.Close()
			}
		}
		return fmt.Errorf("interrupted")
	case <-complete:
		return <-errCh
	}
}
