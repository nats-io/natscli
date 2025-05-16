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

// https://blog.gopheracademy.com/advent-2014/backoff/

import (
	"context"
	"math/rand/v2"
	"time"
)

// BackoffPolicy implements a backoff policy, randomizing its delays
// and saturating at the final value in Millis.
type BackoffPolicy struct {
	Millis []int
}

// DefaultBackoff is the default backoff policy to use
var DefaultBackoff = BackoffPolicy{
	Millis: []int{
		500, 750, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000,
		5500, 5750, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000,
		10500, 10750, 11000, 11500, 12000, 12500, 13000, 13500, 14000, 14500, 15000,
		15500, 15750, 16000, 16500, 17000, 17500, 18000, 18500, 19000, 19500, 20000,
	},
}

// Duration returns the time duration of the n'th wait cycle in a
// backoff policy. This is b.Millis[n], randomized to avoid thundering
// herds.
func (b BackoffPolicy) Duration(n int) time.Duration {
	if n >= len(b.Millis) {
		n = len(b.Millis) - 1
	}

	return time.Duration(jitter(b.Millis[n])) * time.Millisecond
}

// TrySleep sleeps for the duration of the n'th try cycle
// in a way that can be interrupted by the context.  An error is returned
// if the context cancels the sleep
func (b BackoffPolicy) TrySleep(ctx context.Context, n int) error {
	return b.Sleep(ctx, b.Duration(n))
}

// Sleep sleeps for the duration t and can be interrupted by ctx. An error
// is returns if the context cancels the sleep
func (b BackoffPolicy) Sleep(ctx context.Context, t time.Duration) error {
	timer := time.NewTimer(t)

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// For is a for{} loop that stops on context and has a backoff based sleep between loops
// if the context completes the loop ends returning the context error
func (b BackoffPolicy) For(ctx context.Context, cb func(try int) error) error {
	tries := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		tries++

		err := cb(tries)
		if err == nil {
			return nil
		}

		err = b.TrySleep(ctx, tries)
		if err != nil {
			return err
		}
	}
}

// jitter returns a random integer uniformly distributed in the range
// [0.5 * millis .. 1.5 * millis]
func jitter(millis int) int {
	if millis == 0 {
		return 0
	}

	return millis/2 + rand.N(millis)
}
