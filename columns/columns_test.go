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

package columns

import (
	"math"
	"math/big"
	"strings"
	"testing"
	"time"
)

// renderHuman is the default when TESTING env var is not set.
// We call renderHuman directly to avoid the env-var dispatch.
func renderHumanStr(t *testing.T, w *Writer) string {
	t.Helper()
	var buf strings.Builder
	if err := w.renderHuman(&buf); err != nil {
		t.Fatalf("renderHuman: %v", err)
	}
	return buf.String()
}

func renderJSONStr(t *testing.T, w *Writer) string {
	t.Helper()
	var buf strings.Builder
	if err := w.renderJSON(&buf); err != nil {
		t.Fatalf("renderJSON: %v", err)
	}
	return buf.String()
}

func TestF_String(t *testing.T) {
	if got := F("hello"); got != "hello" {
		t.Errorf("got %q", got)
	}
}

func TestF_Bool(t *testing.T) {
	if got := F(true); got != "true" {
		t.Errorf("got %q", got)
	}
	if got := F(false); got != "false" {
		t.Errorf("got %q", got)
	}
}

func TestF_Ints(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{int(1234567), "1,234,567"},
		{int32(1234567), "1,234,567"},
		{int64(1234567), "1,234,567"},
		{uint(1234567), "1,234,567"},
		{uint16(65535), "65,535"},
		{uint32(1234567), "1,234,567"},
		{uint64(1234567), "1,234,567"},
	}
	for _, tc := range cases {
		if got := F(tc.v); got != tc.want {
			t.Errorf("F(%T %v) = %q, want %q", tc.v, tc.v, got, tc.want)
		}
	}
}

func TestF_Uint64_Large(t *testing.T) {
	// Values >= math.MaxInt64 must not be cast to int64 (would overflow).
	large := uint64(math.MaxUint64)
	got := F(large)
	if got == "" {
		t.Fatal("empty result for large uint64")
	}
	// Should contain no commas (falls through to strconv.FormatUint path).
	if strings.Contains(got, ",") {
		t.Errorf("unexpected comma in %q", got)
	}
}

func TestF_Floats(t *testing.T) {
	got32 := F(float32(1234.5))
	if !strings.Contains(got32, "1,234") {
		t.Errorf("float32: got %q", got32)
	}
	got64 := F(float64(1234.5))
	if !strings.Contains(got64, "1,234") {
		t.Errorf("float64: got %q", got64)
	}
}

func TestF_BigInt(t *testing.T) {
	b := new(big.Int).SetInt64(1234567890)
	got := F(b)
	if !strings.Contains(got, "1,234,567,890") {
		t.Errorf("big.Int: got %q", got)
	}
}

func TestF_StringSlice(t *testing.T) {
	got := F([]string{"a", "b", "c"})
	if got != "a, b, c" {
		t.Errorf("got %q", got)
	}
}

func TestF_TimeDuration(t *testing.T) {
	got := F(2 * time.Second)
	if got == "" {
		t.Fatal("empty result for duration")
	}
}

func TestF_Time(t *testing.T) {
	ts := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	got := F(ts)
	if !strings.Contains(got, "2024-01-15") {
		t.Errorf("time: got %q", got)
	}
}

func TestF_Unknown(t *testing.T) {
	type custom struct{ V int }
	got := F(custom{42})
	if got == "" {
		t.Fatal("empty result for unknown type")
	}
}

func TestHumanizeDuration_Never(t *testing.T) {
	if got := HumanizeDuration(math.MaxInt64); got != "never" {
		t.Errorf("got %q, want \"never\"", got)
	}
}

func TestHumanizeDuration_SubMicrosecond(t *testing.T) {
	got := HumanizeDuration(500 * time.Nanosecond)
	if got == "" {
		t.Fatal("empty")
	}
}

func TestHumanizeDuration_SubSecond(t *testing.T) {
	got := HumanizeDuration(250 * time.Millisecond)
	if !strings.HasSuffix(got, "ms") {
		t.Errorf("got %q, want ms suffix", got)
	}
}

func TestHumanizeDuration_Seconds(t *testing.T) {
	got := HumanizeDuration(45 * time.Second)
	if !strings.HasSuffix(got, "s") {
		t.Errorf("got %q", got)
	}
}

func TestHumanizeDuration_Minutes(t *testing.T) {
	got := HumanizeDuration(3*time.Minute + 20*time.Second)
	if !strings.HasPrefix(got, "3m") {
		t.Errorf("got %q", got)
	}
}

func TestHumanizeDuration_Hours(t *testing.T) {
	got := HumanizeDuration(2*time.Hour + 30*time.Minute)
	if !strings.HasPrefix(got, "2h") {
		t.Errorf("got %q", got)
	}
}

func TestHumanizeDuration_Days(t *testing.T) {
	got := HumanizeDuration(3*24*time.Hour + time.Hour)
	if !strings.HasPrefix(got, "3d") {
		t.Errorf("got %q", got)
	}
}

func TestHumanizeDuration_Years(t *testing.T) {
	got := HumanizeDuration(400 * 24 * time.Hour)
	if !strings.HasPrefix(got, "1y") {
		t.Errorf("got %q", got)
	}
}

func TestUtf8StringLen_ASCII(t *testing.T) {
	if n := utf8StringLen("hello"); n != 5 {
		t.Errorf("got %d", n)
	}
}

func TestUtf8StringLen_Multibyte(t *testing.T) {
	// Each of these is 3 bytes in UTF-8 but counts as one rune.
	s := "日本語" // 3 runes, 9 bytes
	if n := utf8StringLen(s); n != 3 {
		t.Errorf("got %d, want 3", n)
	}
}

func TestRenderHuman_Heading(t *testing.T) {
	w := New("My Heading")
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "My Heading") {
		t.Errorf("heading missing: %q", out)
	}
}

func TestRenderHuman_NoHeading(t *testing.T) {
	w := New("")
	w.AddRow("Key", "Value")
	out := renderHumanStr(t, w)
	// Should not start with a blank line.
	if strings.HasPrefix(out, "\n") {
		t.Errorf("unexpected leading newline: %q", out)
	}
}

func TestRenderHuman_SingleRow(t *testing.T) {
	w := New("")
	w.AddRow("Name", "Alice")
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "Name") || !strings.Contains(out, "Alice") {
		t.Errorf("row missing: %q", out)
	}
}

func TestRenderHuman_Alignment(t *testing.T) {
	w := New("")
	w.AddRow("Short", "v1")
	w.AddRow("LongerKey", "v2")
	out := renderHumanStr(t, w)
	lines := nonEmptyLines(out)
	if len(lines) < 2 {
		t.Fatalf("expected 2 lines, got: %q", out)
	}
	// Both separators should be at the same column.
	col0 := strings.Index(lines[0], ":")
	col1 := strings.Index(lines[1], ":")
	if col0 != col1 {
		t.Errorf("separator columns differ: %d vs %d\n%s", col0, col1, out)
	}
}

func TestRenderHuman_EmptyKey(t *testing.T) {
	w := New("")
	w.AddRow("Key", "first")
	w.AddRow("", "continuation")
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "continuation") {
		t.Errorf("continuation line missing: %q", out)
	}
	// Empty-key line must not contain ":"
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "continuation") && strings.Contains(line, ":") {
			t.Errorf("empty-key line should not have separator: %q", line)
		}
	}
}

func TestRenderHuman_SectionTitle(t *testing.T) {
	w := New("")
	w.AddRow("Before", "x")
	w.AddSectionTitle("Section One")
	w.AddRow("After", "y")
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "Section One") {
		t.Errorf("section title missing: %q", out)
	}
}

func TestRenderHuman_Indent(t *testing.T) {
	w := New("")
	w.AddRow("Normal", "a")
	w.Indent(4)
	w.AddRow("Indented", "b")
	out := renderHumanStr(t, w)
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "Indented") {
			if !strings.HasPrefix(line, "    ") {
				t.Errorf("line not indented: %q", line)
			}
		}
	}
}

func TestRenderHuman_Println(t *testing.T) {
	w := New("")
	w.Println("free text line")
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "free text line") {
		t.Errorf("println line missing: %q", out)
	}
}

func TestRenderHuman_PrintlnEmpty(t *testing.T) {
	w := New("")
	w.AddRow("K", "v")
	w.Println()
	w.AddRow("K2", "v2")
	out := renderHumanStr(t, w)
	// Should contain a blank line between the two rows.
	if !strings.Contains(out, "\n\n") {
		t.Errorf("expected blank line: %q", out)
	}
}

func TestRenderHuman_CustomSeparator(t *testing.T) {
	w := New("")
	w.SetSeparator("=")
	w.AddRow("Key", "val")
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "=") {
		t.Errorf("custom separator missing: %q", out)
	}
}

func TestRenderJSON_TopLevel(t *testing.T) {
	w := New("My Header")
	w.AddRow("Name", "Alice")
	out := renderJSONStr(t, w)
	if !strings.Contains(out, `"Name"`) || !strings.Contains(out, `"Alice"`) {
		t.Errorf("row missing from JSON: %q", out)
	}
	if !strings.Contains(out, `"Header"`) {
		t.Errorf("Header key missing from JSON: %q", out)
	}
}

func TestRenderJSON_Sections(t *testing.T) {
	w := New("")
	w.AddSectionTitle("Config")
	w.AddRow("Host", "localhost")
	w.AddRow("Port", "4222")
	out := renderJSONStr(t, w)
	if !strings.Contains(out, `"Config"`) {
		t.Errorf("section missing: %q", out)
	}
	if !strings.Contains(out, `"Host"`) {
		t.Errorf("Host missing: %q", out)
	}
}

func TestRenderJSON_PrintlnBeforeTitle_Dropped(t *testing.T) {
	w := New("")
	w.Println("this should be dropped")
	out := renderJSONStr(t, w)
	if strings.Contains(out, "dropped") {
		t.Errorf("println before title should be dropped in JSON: %q", out)
	}
}

func TestAddRowUnlimited_Shows(t *testing.T) {
	w := New("")
	w.AddRowUnlimited("Limit", -1, -1)
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "unlimited") {
		t.Errorf("expected 'unlimited': %q", out)
	}
}

func TestAddRowUnlimited_Value(t *testing.T) {
	w := New("")
	w.AddRowUnlimited("Limit", int64(100), int64(-1))
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "100") {
		t.Errorf("expected '100': %q", out)
	}
}

func TestAddRowUnlimitedIf_True(t *testing.T) {
	w := New("")
	w.AddRowUnlimitedIf("Limit", 0, true)
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "unlimited") {
		t.Errorf("expected 'unlimited': %q", out)
	}
}

func TestAddRowUnlimitedIf_False(t *testing.T) {
	w := New("")
	w.AddRowUnlimitedIf("Limit", 42, false)
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "42") {
		t.Errorf("expected '42': %q", out)
	}
}

func TestAddRowIf_False(t *testing.T) {
	w := New("")
	w.AddRowIf("Hidden", "secret", false)
	out := renderHumanStr(t, w)
	if strings.Contains(out, "Hidden") {
		t.Errorf("row should be hidden: %q", out)
	}
}

func TestAddRowIfNotEmpty_Empty(t *testing.T) {
	w := New("")
	w.AddRowIfNotEmpty("Tag", "")
	out := renderHumanStr(t, w)
	if strings.Contains(out, "Tag") {
		t.Errorf("empty row should be omitted: %q", out)
	}
}

func TestAddRowIfNotEmpty_NonEmpty(t *testing.T) {
	w := New("")
	w.AddRowIfNotEmpty("Tag", "v1.0")
	out := renderHumanStr(t, w)
	if !strings.Contains(out, "v1.0") {
		t.Errorf("non-empty row missing: %q", out)
	}
}

func TestAddMapInts_SortAscending(t *testing.T) {
	w := New("")
	w.AddMapInts(map[string]int{"b": 2, "a": 1, "c": 3}, true, false)
	out := renderHumanStr(t, w)
	posA := strings.Index(out, "a")
	posB := strings.Index(out, "b")
	posC := strings.Index(out, "c")
	if !(posA < posB && posB < posC) {
		t.Errorf("expected ascending sort a<b<c, got:\n%s", out)
	}
}

func TestAddMapInts_SortDescending(t *testing.T) {
	w := New("")
	w.AddMapInts(map[string]int{"b": 2, "a": 1, "c": 3}, true, true)
	out := renderHumanStr(t, w)
	posA := strings.Index(out, "a")
	posB := strings.Index(out, "b")
	posC := strings.Index(out, "c")
	if !(posC < posB && posB < posA) {
		t.Errorf("expected descending sort c>b>a, got:\n%s", out)
	}
}

func TestAddStringsAsValue_UTF8_NoCorruption(t *testing.T) {
	// Build a string of 200 multi-byte runes so truncation will be triggered
	// when screenWidth returns a small value (tests run without a terminal,
	// so GetSize fails and returns 80, giving maxLen=50).
	s := strings.Repeat("日", 200) // 200 runes, 600 bytes
	w := New("")
	w.AddStringsAsValue("Key", []string{s})
	out := renderHumanStr(t, w)
	// The output must be valid UTF-8 — no garbled bytes.
	if !isValidUTF8(out) {
		t.Errorf("output contains invalid UTF-8 after truncation")
	}
}

func TestAddMapStringsAsValue_UTF8_NoCorruption(t *testing.T) {
	s := strings.Repeat("中", 200)
	w := New("")
	w.AddMapStringsAsValue("Key", map[string]string{"label": s})
	out := renderHumanStr(t, w)
	if !isValidUTF8(out) {
		t.Errorf("output contains invalid UTF-8 after truncation")
	}
}

func TestAddMapStrings_UTF8_NoCorruption(t *testing.T) {
	s := strings.Repeat("한", 200)
	w := New("")
	w.AddMapStrings(map[string]string{"label": s})
	out := renderHumanStr(t, w)
	if !isValidUTF8(out) {
		t.Errorf("output contains invalid UTF-8 after truncation")
	}
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.TrimSpace(l) != "" {
			out = append(out, l)
		}
	}
	return out
}

func isValidUTF8(s string) bool {
	for _, r := range s {
		if r == '\uFFFD' {
			return false
		}
	}
	return true
}
