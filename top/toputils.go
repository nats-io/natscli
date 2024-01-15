package top

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const DisplaySubscriptions = 1

type Engine struct {
	Nc           *nats.Conn
	Host         string
	Conns        int
	SortOpt      server.SortOpt
	Delay        int
	DisplaySubs  bool
	StatsCh      chan *Stats
	ShutdownCh   chan struct{}
	LastStats    *Stats
	LastPollTime time.Time
	ShowRates    bool
	LastConnz    map[uint64]*server.ConnInfo
	Trace        bool
}

func NewEngine(nc *nats.Conn, host string, conns int, delay int, trace bool) *Engine {
	return &Engine{
		Host:       host,
		Nc:         nc,
		Trace:      trace,
		Conns:      conns,
		Delay:      delay,
		StatsCh:    make(chan *Stats),
		ShutdownCh: make(chan struct{}),
		LastConnz:  make(map[uint64]*server.ConnInfo),
	}
}

type serverAPIResponse struct {
	Server *server.ServerInfo `json:"server"`
	Data   json.RawMessage    `json:"data,omitempty"`
	Error  *server.ApiError   `json:"error,omitempty"`
}

func (e *Engine) doReq(path string, opts any) (*serverAPIResponse, error) {
	var req []byte
	var err error
	if opts != nil {
		req, err = json.Marshal(opts)
		if err != nil {
			return nil, err
		}
	}

	subj := fmt.Sprintf("$SYS.REQ.SERVER.PING.%s", path)
	msg := nats.NewMsg(subj)
	msg.Header.Set("Accept-Encoding", "snappy")

	if e.Trace {
		log.Printf(">>> %s: %s", subj, string(req))
	}

	res, err := e.Nc.Request(subj, req, time.Second)
	if errors.Is(err, nats.ErrNoResponders) {
		return nil, fmt.Errorf("no results received, ensure the account used has system privileges and appropriate permissions")
	}
	if err != nil {
		return nil, err
	}

	data := res.Data
	compressed := res.Header.Get("Content-Encoding") == "snappy"
	if compressed {
		ud, err := io.ReadAll(s2.NewReader(bytes.NewBuffer(data)))
		if err != nil {
			return nil, err
		}
		data = ud
	}

	if e.Trace {
		if compressed {
			log.Printf("<<< (%dB -> %dB) %s", len(res.Data), len(data), string(data))
		} else {
			log.Printf("<<< (%dB) %s", len(data), string(data))
		}
	}

	out := &serverAPIResponse{}
	err = json.Unmarshal(data, out)
	if err != nil {
		return nil, err
	}

	if out.Error != nil {
		return nil, fmt.Errorf(out.Error.Error())
	}

	return out, nil
}

// Request takes a path and options, and returns a Stats struct
// with with either connz or varz
func (e *Engine) Request(path string) (interface{}, error) {
	var statz any

	switch path {
	case "VARZ":
		opts := &server.VarzEventOptions{
			EventFilterOptions: server.EventFilterOptions{
				Name: e.Host,
			},
		}

		out, err := e.doReq(path, opts)
		if err != nil {
			return nil, err
		}

		statz = &server.Varz{}
		err = json.Unmarshal(out.Data, &statz)
		if err != nil {
			return nil, err
		}

	case "CONNZ":
		opts := &server.ConnzEventOptions{
			ConnzOptions: server.ConnzOptions{
				Limit:         e.Conns,
				Sort:          e.SortOpt,
				Subscriptions: e.DisplaySubs,
			},
			EventFilterOptions: server.EventFilterOptions{
				Name: e.Host,
			},
		}

		out, err := e.doReq(path, opts)
		if err != nil {
			return nil, err
		}

		statz = &server.Connz{}
		err = json.Unmarshal(out.Data, &statz)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("invalid server request %q", path)
	}

	return statz, nil
}

// MonitorStats is ran as a goroutine and takes options
// which can modify how poll values then sends to channel.
func (e *Engine) MonitorStats() error {
	// Initial fetch.
	e.StatsCh <- e.fetchStats()

	delay := time.Duration(e.Delay) * time.Second
	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	for {
		select {
		case <-e.ShutdownCh:
			return nil
		case <-ticker.C:
			e.StatsCh <- e.fetchStats()
		}
	}
}

func (e *Engine) FetchStatsSnapshot() *Stats {
	return e.fetchStats()
}

var errDud = fmt.Errorf("")

func (e *Engine) fetchStats() *Stats {
	var inMsgsDelta int64
	var outMsgsDelta int64
	var inBytesDelta int64
	var outBytesDelta int64

	var inMsgsLastVal int64
	var outMsgsLastVal int64
	var inBytesLastVal int64
	var outBytesLastVal int64

	var inMsgsRate float64
	var outMsgsRate float64
	var inBytesRate float64
	var outBytesRate float64

	stats := &Stats{
		Varz:  &server.Varz{},
		Connz: &server.Connz{},
		Rates: &Rates{},
		Error: errDud,
	}

	// Get /varz
	{
		result, err := e.Request("VARZ")
		if err != nil {
			stats.Error = err
			return stats
		}

		if varz, ok := result.(*server.Varz); ok {
			stats.Varz = varz
		}
	}

	// Get /connz
	{
		result, err := e.Request("CONNZ")
		if err != nil {
			stats.Error = err
			return stats
		}

		if connz, ok := result.(*server.Connz); ok {
			stats.Connz = connz
		}
	}

	var isFirstTime bool
	if e.LastStats != nil {
		inMsgsLastVal = e.LastStats.Varz.InMsgs
		outMsgsLastVal = e.LastStats.Varz.OutMsgs
		inBytesLastVal = e.LastStats.Varz.InBytes
		outBytesLastVal = e.LastStats.Varz.OutBytes
	} else {
		isFirstTime = true
	}

	// Periodic snapshot to get per sec metrics
	inMsgsVal := stats.Varz.InMsgs
	outMsgsVal := stats.Varz.OutMsgs
	inBytesVal := stats.Varz.InBytes
	outBytesVal := stats.Varz.OutBytes

	inMsgsDelta = inMsgsVal - inMsgsLastVal
	outMsgsDelta = outMsgsVal - outMsgsLastVal
	inBytesDelta = inBytesVal - inBytesLastVal
	outBytesDelta = outBytesVal - outBytesLastVal

	inMsgsLastVal = inMsgsVal
	outMsgsLastVal = outMsgsVal
	inBytesLastVal = inBytesVal
	outBytesLastVal = outBytesVal

	// Snapshot per sec metrics for connections.
	connz := make(map[uint64]*server.ConnInfo)
	for _, conn := range stats.Connz.Conns {
		connz[conn.Cid] = conn
	}

	// Calculate rates but the first time
	if !isFirstTime {
		tdelta := stats.Varz.Now.Sub(e.LastStats.Varz.Now)

		inMsgsRate = float64(inMsgsDelta) / tdelta.Seconds()
		outMsgsRate = float64(outMsgsDelta) / tdelta.Seconds()
		inBytesRate = float64(inBytesDelta) / tdelta.Seconds()
		outBytesRate = float64(outBytesDelta) / tdelta.Seconds()
	}
	rates := &Rates{
		InMsgsRate:   inMsgsRate,
		OutMsgsRate:  outMsgsRate,
		InBytesRate:  inBytesRate,
		OutBytesRate: outBytesRate,
		Connections:  make(map[uint64]*ConnRates),
	}

	// Measure per connection metrics.
	for cid, conn := range connz {
		cr := &ConnRates{
			InMsgsRate:   0,
			OutMsgsRate:  0,
			InBytesRate:  0,
			OutBytesRate: 0,
		}
		lconn, wasConnected := e.LastConnz[cid]
		if wasConnected {
			cr.InMsgsRate = float64(conn.InMsgs - lconn.InMsgs)
			cr.OutMsgsRate = float64(conn.OutMsgs - lconn.OutMsgs)
			cr.InBytesRate = float64(conn.InBytes - lconn.InBytes)
			cr.OutBytesRate = float64(conn.OutBytes - lconn.OutBytes)
		}
		rates.Connections[cid] = cr
	}

	stats.Rates = rates

	// Snapshot stats.
	e.LastStats = stats
	e.LastPollTime = time.Now()
	e.LastConnz = connz

	return stats
}

// Stats represents the monitored data from a NATS server.
type Stats struct {
	Varz  *server.Varz
	Connz *server.Connz
	Rates *Rates
	Error error
}

// Rates represents the tracked in/out msgs and bytes flow
// from a NATS server.
type Rates struct {
	InMsgsRate   float64
	OutMsgsRate  float64
	InBytesRate  float64
	OutBytesRate float64
	Connections  map[uint64]*ConnRates
}

type ConnRates struct {
	InMsgsRate   float64
	OutMsgsRate  float64
	InBytesRate  float64
	OutBytesRate float64
}

const kibibyte = 1024
const mebibyte = 1024 * 1024
const gibibyte = 1024 * 1024 * 1024

// Psize takes a float and returns a human readable string (Used for bytes).
func Psize(displayRawValue bool, s int64) string {
	size := float64(s)

	if displayRawValue || size < kibibyte {
		return fmt.Sprintf("%.0f", size)
	}

	if size < mebibyte {
		return fmt.Sprintf("%.1fK", size/kibibyte)
	}

	if size < gibibyte {
		return fmt.Sprintf("%.1fM", size/mebibyte)
	}

	return fmt.Sprintf("%.1fG", size/gibibyte)
}

const k = 1000
const m = k * 1000
const b = m * 1000
const t = b * 1000

// Nsize takes a float and returns a human readable string.
func Nsize(displayRawValue bool, s int64) string {
	size := float64(s)

	switch {
	case displayRawValue || size < k:
		return fmt.Sprintf("%.0f", size)
	case size < m:
		return fmt.Sprintf("%.1fK", size/k)
	case size < b:
		return fmt.Sprintf("%.1fM", size/m)
	case size < t:
		return fmt.Sprintf("%.1fB", size/b)
	default:
		return fmt.Sprintf("%.1fT", size/t)
	}
}
