// Copyright 2024-2025 The NATS Authors
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
	"testing"
)

func TestIntRangeValidator(t *testing.T) {
	tests := []struct {
		name    string
		min     int64
		max     int64
		input   string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid value in range",
			min:     1,
			max:     10,
			input:   "5",
			wantErr: false,
		},
		{
			name:    "valid minimum value",
			min:     1,
			max:     10,
			input:   "1",
			wantErr: false,
		},
		{
			name:    "valid maximum value",
			min:     1,
			max:     10,
			input:   "10",
			wantErr: false,
		},
		{
			name:    "value below minimum",
			min:     1,
			max:     10,
			input:   "0",
			wantErr: true,
			errMsg:  "must be greater than or equal to 1",
		},
		{
			name:    "value above maximum",
			min:     1,
			max:     10,
			input:   "11",
			wantErr: true,
			errMsg:  "must be less than or equal to 10",
		},
		{
			name:    "negative value below minimum",
			min:     1,
			max:     10,
			input:   "-5",
			wantErr: true,
			errMsg:  "must be greater than or equal to 1",
		},
		{
			name:    "invalid non-numeric input",
			min:     1,
			max:     10,
			input:   "abc",
			wantErr: true,
		},
		{
			name:    "invalid empty input",
			min:     1,
			max:     10,
			input:   "",
			wantErr: true,
		},
		{
			name:    "invalid float input",
			min:     1,
			max:     10,
			input:   "5.5",
			wantErr: true,
		},
		{
			name:    "negative range - valid",
			min:     -10,
			max:     -1,
			input:   "-5",
			wantErr: false,
		},
		{
			name:    "negative range - too high",
			min:     -10,
			max:     -1,
			input:   "0",
			wantErr: true,
			errMsg:  "must be less than or equal to -1",
		},
		{
			name:    "large range - valid",
			min:     0,
			max:     1000000,
			input:   "500000",
			wantErr: false,
		},
		{
			name:    "single value range - valid",
			min:     42,
			max:     42,
			input:   "42",
			wantErr: false,
		},
		{
			name:    "single value range - invalid",
			min:     42,
			max:     42,
			input:   "43",
			wantErr: true,
			errMsg:  "must be less than or equal to 42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := Int64RangeValidator(tt.min, tt.max)
			err := validator(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("expected error message %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error but got: %v", err)
				}
			}
		})
	}
}
