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

package jsch

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
)

// MsgInfo holds metadata about a message that was received from JetStream
type MsgInfo struct {
	stream    string
	consumer  string
	sSeq      int
	cSeq      int
	delivered int
}

func (i *MsgInfo) Stream() string {
	return i.stream
}

func (i *MsgInfo) Consumer() string {
	return i.consumer
}

func (i *MsgInfo) StreamSequence() int {
	return i.sSeq
}

func (i *MsgInfo) ConsumerSequence() int {
	return i.cSeq
}

func (i *MsgInfo) Delivered() int {
	return i.delivered
}

// ParseJSMsgMetadata parse the reply subject metadata to determine message metadata
func ParseJSMsgMetadata(m *nats.Msg) (info *MsgInfo, err error) {
	if len(m.Reply) == 0 {
		return nil, fmt.Errorf("reply subject is not an Ack")
	}

	parts := strings.Split(m.Reply, ".")
	c := len(parts)

	if c != 7 || parts[0] != "$JS" || parts[1] != "ACK" {
		return nil, fmt.Errorf("message metadata does not appear to be an ACK")
	}

	stream := parts[c-5]
	consumer := parts[c-4]
	delivered, _ := strconv.Atoi(parts[c-3])
	streamSeq, _ := strconv.Atoi(parts[c-2])
	consumerSeq, _ := strconv.Atoi(parts[c-1])

	return &MsgInfo{stream, consumer, streamSeq, consumerSeq, delivered}, nil
}
