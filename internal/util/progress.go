// Copyright 2024 The NATS Authors
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
	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/v6/progress"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/nats-io/natscli/options"
	"time"
)

var ProgressUnitsIBytes = progress.Units{
	Notation:         "",
	NotationPosition: progress.UnitsNotationPositionBefore,
	Formatter:        func(v int64) string { return humanize.IBytes(uint64(v)) },
}

func NewProgress(opts *options.Options, tracker *progress.Tracker) (progress.Writer, *progress.Tracker, error) {
	progbar := progress.NewWriter()
	natsStyle := progress.StyleBlocks
	natsStyle.Visibility.ETA = false
	natsStyle.Visibility.Speed = true
	natsStyle.Colors.Tracker = contextColor(opts)
	natsStyle.Options.Separator = " "
	natsStyle.Options.TimeInProgressPrecision = time.Millisecond
	progbar.SetStyle(natsStyle)
	progbar.SetAutoStop(true)
	pw := ProgressWidth()
	progbar.SetTrackerLength(pw / 2)
	progbar.SetNumTrackersExpected(1)
	progbar.SetUpdateFrequency(250 * time.Millisecond)

	progbar.AppendTracker(tracker)
	go progbar.Render()

	return progbar, tracker, nil
}

func contextColor(opts *options.Options) text.Colors {
	cs := opts.Config.ColorScheme()
	s, ok := styles[cs]
	if !ok {
		return text.Colors{text.FgWhite}
	}
	return s.Color.Border
}
