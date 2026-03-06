package serverdata

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// mockReqFn returns a RequestFunc that records calls and returns preset responses.
func mockReqFn(responses [][]byte, err error) (RequestFunc, *[]mockCall) {
	var calls []mockCall
	fn := func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		calls = append(calls, mockCall{req: req, subj: subj, waitFor: waitFor})
		return responses, err
	}
	return fn, &calls
}

type mockCall struct {
	req     any
	subj    string
	waitFor int
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestServerVarz(t *testing.T) {
	resp := &server.ServerAPIVarzResponse{
		Server: &server.ServerInfo{Name: "srv-1"},
		Data:   &server.Varz{MaxConn: 100},
	}
	reqFn, calls := mockReqFn([][]byte{mustMarshal(t, resp)}, nil)
	src := NewServer(nil, reqFn, 3)

	opts := server.VarzEventOptions{}
	results, err := src.Varz(opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Server.Name != "srv-1" {
		t.Errorf("expected server name srv-1, got %s", results[0].Server.Name)
	}
	if results[0].Data.MaxConn != 100 {
		t.Errorf("expected MaxConn 100, got %d", results[0].Data.MaxConn)
	}
	if len(*calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(*calls))
	}
	if (*calls)[0].subj != "$SYS.REQ.SERVER.PING.VARZ" {
		t.Errorf("expected VARZ subject, got %s", (*calls)[0].subj)
	}
	if (*calls)[0].waitFor != 3 {
		t.Errorf("expected waitFor 3, got %d", (*calls)[0].waitFor)
	}
}

func TestServerConnz(t *testing.T) {
	resp := &server.ServerAPIConnzResponse{
		Server: &server.ServerInfo{Name: "srv-2"},
		Data:   &server.Connz{NumConns: 42},
	}
	reqFn, _ := mockReqFn([][]byte{mustMarshal(t, resp)}, nil)
	src := NewServer(nil, reqFn, 0)

	results, err := src.Connz(server.ConnzEventOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Data.NumConns != 42 {
		t.Errorf("expected 42 connections, got %d", results[0].Data.NumConns)
	}
}

func TestServerStatz(t *testing.T) {
	resp := &server.ServerStatsMsg{
		Server: server.ServerInfo{Name: "srv-3", Cluster: "c1"},
	}
	reqFn, calls := mockReqFn([][]byte{mustMarshal(t, resp)}, nil)
	src := NewServer(nil, reqFn, 5)

	results, err := src.Statz(server.StatszEventOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Server.Name != "srv-3" {
		t.Errorf("expected server name srv-3, got %s", results[0].Server.Name)
	}
	if (*calls)[0].subj != "$SYS.REQ.SERVER.PING" {
		t.Errorf("expected PING subject, got %s", (*calls)[0].subj)
	}
}

func TestServerMultipleResponses(t *testing.T) {
	r1 := mustMarshal(t, &server.ServerAPIVarzResponse{
		Server: &server.ServerInfo{Name: "srv-a"},
		Data:   &server.Varz{},
	})
	r2 := mustMarshal(t, &server.ServerAPIVarzResponse{
		Server: &server.ServerInfo{Name: "srv-b"},
		Data:   &server.Varz{},
	})
	reqFn, _ := mockReqFn([][]byte{r1, r2}, nil)
	src := NewServer(nil, reqFn, 2)

	results, err := src.Varz(server.VarzEventOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].Server.Name != "srv-a" {
		t.Errorf("expected srv-a, got %s", results[0].Server.Name)
	}
	if results[1].Server.Name != "srv-b" {
		t.Errorf("expected srv-b, got %s", results[1].Server.Name)
	}
}

func TestServerRequestError(t *testing.T) {
	reqFn, _ := mockReqFn(nil, fmt.Errorf("connection lost"))
	src := NewServer(nil, reqFn, 1)

	_, err := src.Varz(server.VarzEventOptions{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestServerUnmarshalError(t *testing.T) {
	reqFn, _ := mockReqFn([][]byte{[]byte("not json")}, nil)
	src := NewServer(nil, reqFn, 1)

	_, err := src.Varz(server.VarzEventOptions{})
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestServerClose(t *testing.T) {
	src := NewServer(nil, nil, 0)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
}

// mockReqFnSequence returns a RequestFunc that returns different responses on each call.
func mockReqFnSequence(sequence []mockResponse) RequestFunc {
	idx := 0
	return func(req any, subj string, waitFor int, nc *nats.Conn) ([][]byte, error) {
		if idx >= len(sequence) {
			return nil, fmt.Errorf("unexpected call %d", idx)
		}
		resp := sequence[idx]
		idx++
		return resp.data, resp.err
	}
}

type mockResponse struct {
	data [][]byte
	err  error
}

func TestServerCollectAccountsSingleServer(t *testing.T) {
	resp := &server.ServerAPIJszResponse{
		Server: &server.ServerInfo{Name: "srv-1"},
		Data: &server.JSInfo{
			AccountDetails: []*server.AccountDetail{
				{Name: "beta", Streams: []server.StreamDetail{{Name: "s1"}}},
				{Name: "alpha", Streams: []server.StreamDetail{{Name: "s2"}}},
			},
		},
	}
	reqFn, _ := mockReqFn([][]byte{mustMarshal(t, resp)}, nil)
	src := NewServer(nil, reqFn, 1)

	accounts, err := src.CollectAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 2 {
		t.Fatalf("expected 2 accounts, got %d", len(accounts))
	}
	// Should be sorted by name
	if accounts[0].Name != "alpha" {
		t.Errorf("expected first account alpha, got %s", accounts[0].Name)
	}
	if accounts[1].Name != "beta" {
		t.Errorf("expected second account beta, got %s", accounts[1].Name)
	}
}

func TestServerCollectAccountsMergesAcrossServers(t *testing.T) {
	r1 := &server.ServerAPIJszResponse{
		Server: &server.ServerInfo{Name: "srv-1"},
		Data: &server.JSInfo{
			AccountDetails: []*server.AccountDetail{
				{Name: "acct-A", Streams: []server.StreamDetail{{Name: "s1"}, {Name: "s2"}}},
			},
		},
	}
	r2 := &server.ServerAPIJszResponse{
		Server: &server.ServerInfo{Name: "srv-2"},
		Data: &server.JSInfo{
			AccountDetails: []*server.AccountDetail{
				{Name: "acct-A", Streams: []server.StreamDetail{{Name: "s2"}, {Name: "s3"}}},
			},
		},
	}
	reqFn, _ := mockReqFn([][]byte{mustMarshal(t, r1), mustMarshal(t, r2)}, nil)
	src := NewServer(nil, reqFn, 2)

	accounts, err := src.CollectAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 merged account, got %d", len(accounts))
	}
	// s1, s2, s3 -- s2 deduplicated
	if len(accounts[0].Streams) != 3 {
		t.Errorf("expected 3 deduplicated streams, got %d", len(accounts[0].Streams))
	}
	// Streams should be sorted
	if accounts[0].Streams[0].Name != "s1" || accounts[0].Streams[1].Name != "s2" || accounts[0].Streams[2].Name != "s3" {
		names := make([]string, len(accounts[0].Streams))
		for i, s := range accounts[0].Streams {
			names[i] = s.Name
		}
		t.Errorf("expected sorted [s1 s2 s3], got %v", names)
	}
}

func TestServerCollectAccountsPaging(t *testing.T) {
	// Build a response with exactly 1024 accounts to trigger paging
	details := make([]*server.AccountDetail, 1024)
	for i := range details {
		details[i] = &server.AccountDetail{Name: fmt.Sprintf("acct-%04d", i)}
	}
	initialResp := &server.ServerAPIJszResponse{
		Server: &server.ServerInfo{Name: "srv-1"},
		Data:   &server.JSInfo{AccountDetails: details},
	}

	// Second page has fewer than 1024, ending paging
	page2 := &server.ServerAPIJszResponse{
		Server: &server.ServerInfo{Name: "srv-1"},
		Data: &server.JSInfo{
			AccountDetails: []*server.AccountDetail{
				{Name: "acct-extra"},
			},
		},
	}

	seq := mockReqFnSequence([]mockResponse{
		{data: [][]byte{mustMarshal(t, initialResp)}},
		{data: [][]byte{mustMarshal(t, page2)}},
	})
	src := NewServer(nil, seq, 1)

	accounts, err := src.CollectAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1025 {
		t.Fatalf("expected 1025 accounts, got %d", len(accounts))
	}
}

func TestServerSubjects(t *testing.T) {
	tests := []struct {
		name   string
		call   func(ds DataSource) error
		expect string
	}{
		{"Varz", func(ds DataSource) error { _, err := ds.Varz(server.VarzEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.VARZ"},
		{"Connz", func(ds DataSource) error { _, err := ds.Connz(server.ConnzEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.CONNZ"},
		{"Routez", func(ds DataSource) error { _, err := ds.Routez(server.RoutezEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.ROUTEZ"},
		{"Gatewayz", func(ds DataSource) error { _, err := ds.Gatewayz(server.GatewayzEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.GATEWAYZ"},
		{"Leafz", func(ds DataSource) error { _, err := ds.Leafz(server.LeafzEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.LEAFZ"},
		{"Subsz", func(ds DataSource) error { _, err := ds.Subsz(server.SubszEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.SUBSZ"},
		{"Jsz", func(ds DataSource) error { _, err := ds.Jsz(server.JszEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.JSZ"},
		{"Healthz", func(ds DataSource) error { _, err := ds.Healthz(server.HealthzEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.HEALTHZ"},
		{"Accountz", func(ds DataSource) error { _, err := ds.Accountz(server.AccountzEventOptions{}); return err }, "$SYS.REQ.SERVER.PING.ACCOUNTZ"},
		{"Statz", func(ds DataSource) error { _, err := ds.Statz(server.StatszEventOptions{}); return err }, "$SYS.REQ.SERVER.PING"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := []byte(`{}`)
			reqFn, calls := mockReqFn([][]byte{resp}, nil)
			src := NewServer(nil, reqFn, 1)

			_ = tc.call(src)

			if len(*calls) != 1 {
				t.Fatalf("expected 1 call, got %d", len(*calls))
			}
			if (*calls)[0].subj != tc.expect {
				t.Errorf("expected subject %s, got %s", tc.expect, (*calls)[0].subj)
			}
		})
	}
}

func TestServerSubject(t *testing.T) {
	tests := []struct {
		name     string
		filter   server.EventFilterOptions
		endpoint string
		expect   string
	}{
		{"broadcast", server.EventFilterOptions{}, "VARZ", "$SYS.REQ.SERVER.PING.VARZ"},
		{"name without exact", server.EventFilterOptions{Name: "srv1"}, "VARZ", "$SYS.REQ.SERVER.PING.VARZ"},
		{"name with exact", server.EventFilterOptions{Name: "srv1", ExactMatch: true}, "VARZ", "$SYS.REQ.SERVER.srv1.VARZ"},
		{"id with exact", server.EventFilterOptions{Name: "NABC123", ExactMatch: true}, "HEALTHZ", "$SYS.REQ.SERVER.NABC123.HEALTHZ"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := serverSubject(tc.filter, tc.endpoint)
			if got != tc.expect {
				t.Errorf("expected %s, got %s", tc.expect, got)
			}
		})
	}
}
