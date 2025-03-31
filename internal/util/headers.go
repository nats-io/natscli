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

package util

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"net/textproto"
	"strings"
	"text/template"
	"time"
)

// ParseStringsToHeader creates a nats.Header from a list of strings like X:Y
func ParseStringsToHeader(hdrs []string, seq int) (nats.Header, error) {
	res := nats.Header{}

	err := parseStringsToHeader(hdrs, seq, res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// ParseStringsToMsgHeader parsers strings of headers like X:Y into a supplied msg headers
func ParseStringsToMsgHeader(hdrs []string, seq int, msg *nats.Msg) error {
	err := parseStringsToHeader(hdrs, seq, msg.Header)
	if err != nil {
		return err
	}

	return nil
}

func parseStringsToHeader(hdrs []string, seq int, res nats.Header) error {
	for _, hdr := range hdrs {
		parts := strings.SplitN(hdr, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid header %q", hdr)
		}

		val, err := PubReplyBodyTemplate(strings.TrimSpace(parts[1]), "", seq)
		if err != nil {
			return fmt.Errorf("failed to parse Header template for %s: %s", parts[0], err)
		}

		res.Add(strings.TrimSpace(parts[0]), string(val))
	}

	return nil
}

type pubData struct {
	Cnt       int
	Count     int
	Unix      int64
	UnixNano  int64
	TimeStamp string
	Time      string
	Request   string
}

func (p *pubData) ID() string {
	return nuid.Next()
}

// PubReplyBodyTemplate parses a message body using the usual template functions we support as standard
func PubReplyBodyTemplate(body string, request string, ctr int) ([]byte, error) {
	now := time.Now()
	funcMap := template.FuncMap{
		"Random":    RandomString,
		"Count":     func() int { return ctr },
		"Cnt":       func() int { return ctr },
		"Unix":      func() int64 { return now.Unix() },
		"UnixNano":  func() int64 { return now.UnixNano() },
		"TimeStamp": func() string { return now.Format(time.RFC3339) },
		"Time":      func() string { return now.Format(time.Kitchen) },
		"ID":        func() string { return nuid.Next() },
	}

	if request != "" {
		funcMap["Request"] = func() string { return request }
	}

	templ, err := template.New("body").Funcs(funcMap).Parse(body)
	if err != nil {
		return []byte(body), err
	}

	var b bytes.Buffer
	err = templ.Execute(&b, &pubData{
		Cnt:       ctr,
		Count:     ctr,
		Unix:      now.Unix(),
		UnixNano:  now.UnixNano(),
		TimeStamp: now.Format(time.RFC3339),
		Time:      now.Format(time.Kitchen),
		Request:   request,
	})
	if err != nil {
		return []byte(body), err
	}

	return b.Bytes(), nil
}

const (
	hdrLine   = "NATS/1.0\r\n"
	crlf      = "\r\n"
	hdrPreEnd = len(hdrLine) - len(crlf)
	statusLen = 3
	statusHdr = "Status"
	descrHdr  = "Description"
)

// DecodeHeadersMsg parses the data that includes headers into a header. Copied from nats.go
func DecodeHeadersMsg(data []byte) (nats.Header, error) {
	tp := textproto.NewReader(bufio.NewReader(bytes.NewReader(data)))
	l, err := tp.ReadLine()
	if err != nil || len(l) < hdrPreEnd || l[:hdrPreEnd] != hdrLine[:hdrPreEnd] {
		return nil, nats.ErrBadHeaderMsg
	}

	mh, err := readMIMEHeader(tp)
	if err != nil {
		return nil, err
	}

	// Check if we have an inlined status.
	if len(l) > hdrPreEnd {
		var description string
		status := strings.TrimSpace(l[hdrPreEnd:])
		if len(status) != statusLen {
			description = strings.TrimSpace(status[statusLen:])
			status = status[:statusLen]
		}
		mh.Add(statusHdr, status)
		if len(description) > 0 {
			mh.Add(descrHdr, description)
		}
	}
	return nats.Header(mh), nil
}

// copied from nats.go
func readMIMEHeader(tp *textproto.Reader) (textproto.MIMEHeader, error) {
	m := make(textproto.MIMEHeader)
	for {
		kv, err := tp.ReadLine()
		if len(kv) == 0 {
			return m, err
		}

		// Process key fetching original case.
		i := bytes.IndexByte([]byte(kv), ':')
		if i < 0 {
			return nil, nats.ErrBadHeaderMsg
		}
		key := kv[:i]
		if key == "" {
			// Skip empty keys.
			continue
		}
		i++
		for i < len(kv) && (kv[i] == ' ' || kv[i] == '\t') {
			i++
		}
		value := string(kv[i:])
		m[key] = append(m[key], value)
		if err != nil {
			return m, err
		}
	}
}
