package top

import (
	"fmt"
	"testing"
)

func TestPsize(t *testing.T) {
	const kibibyte = 1024
	const mebibyte = 1024 * 1024
	const gibibyte = 1024 * 1024 * 1024

	type Args struct {
		displayRawBytes bool
		input           int64
	}

	testcases := map[string]struct {
		args Args
		want string
	}{
		"given input 1023 and display_raw_bytes false": {
			args: Args{
				input:           int64(1023),
				displayRawBytes: false,
			},
			want: "1023",
		},
		"given input kibibyte and display_raw_bytes false": {
			args: Args{
				input:           int64(kibibyte),
				displayRawBytes: false,
			},
			want: "1.0K",
		},
		"given input mebibyte and display_raw_bytes false": {
			args: Args{
				input:           int64(mebibyte),
				displayRawBytes: false,
			},
			want: "1.0M",
		},
		"given input gibibyte and display_raw_bytes false": {
			args: Args{
				input:           int64(gibibyte),
				displayRawBytes: false,
			},
			want: "1.0G",
		},

		"given input 1023 and display_raw_bytes true": {
			args: Args{
				input:           int64(1023),
				displayRawBytes: true,
			},
			want: "1023",
		},
		"given input kibibyte and display_raw_bytes true": {
			args: Args{
				input:           int64(kibibyte),
				displayRawBytes: true,
			},
			want: fmt.Sprintf("%d", kibibyte),
		},
		"given input mebibyte and display_raw_bytes true": {
			args: Args{
				input:           int64(mebibyte),
				displayRawBytes: true,
			},
			want: fmt.Sprintf("%d", mebibyte),
		},
		"given input gibibyte and display_raw_bytes true": {
			args: Args{
				input:           int64(gibibyte),
				displayRawBytes: true,
			},
			want: fmt.Sprintf("%d", gibibyte),
		},
	}

	for name, testcase := range testcases {
		t.Run(name, func(t *testing.T) {
			got := Psize(testcase.args.displayRawBytes, testcase.args.input)

			if got != testcase.want {
				t.Errorf("wanted %q, got %q", testcase.want, got)
			}
		})
	}
}

func TestNsize(t *testing.T) {
	type Args struct {
		displayRawBytes bool
		input           int64
	}

	testcases := map[string]struct {
		args Args
		want string
	}{
		"given input 999 and display_raw_bytes false": {
			args: Args{
				input:           int64(999),
				displayRawBytes: false,
			},
			want: "999",
		},
		"given input 1000 and display_raw_bytes false": {
			args: Args{
				input:           int64(1000),
				displayRawBytes: false,
			},
			want: "1.0K",
		},
		"given input 1_000_000 and display_raw_bytes false": {
			args: Args{
				input:           int64(1_000_000),
				displayRawBytes: false,
			},
			want: "1.0M",
		},
		"given input 1_000_000_000 and display_raw_bytes false": {
			args: Args{
				input:           int64(1_000_000_000),
				displayRawBytes: false,
			},
			want: "1.0B",
		},
		"given input 1_000_000_000_000 and display_raw_bytes false": {
			args: Args{
				input:           int64(1_000_000_000_000),
				displayRawBytes: false,
			},
			want: "1.0T",
		},

		"given input 999 and display_raw_bytes true": {
			args: Args{
				input:           int64(999),
				displayRawBytes: true,
			},
			want: "999",
		},
		"given input 1000 and display_raw_bytes true": {
			args: Args{
				input:           int64(1000),
				displayRawBytes: true,
			},
			want: "1000",
		},
		"given input 1_000_000 and display_raw_bytes true": {
			args: Args{
				input:           int64(1_000_000),
				displayRawBytes: true,
			},
			want: "1000000",
		},
		"given input 1_000_000_000 and display_raw_bytes true": {
			args: Args{
				input:           int64(1_000_000_000),
				displayRawBytes: true,
			},
			want: "1000000000",
		},
		"given input 1_000_000_000_000 and display_raw_bytes true": {
			args: Args{
				input:           int64(1_000_000_000_000),
				displayRawBytes: true,
			},
			want: "1000000000000",
		},
	}

	for name, testcase := range testcases {
		t.Run(name, func(t *testing.T) {
			got := Nsize(testcase.args.displayRawBytes, testcase.args.input)

			if got != testcase.want {
				t.Errorf("wanted %q, got %q", testcase.want, got)
			}
		})
	}
}
