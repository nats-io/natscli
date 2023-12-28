// Copyright (c) 2015-2023 The NATS Authors

package top

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	ui "gopkg.in/gizak/termui.v1"
)

func SaveStatsSnapshotToFile(engine *Engine, outputFile string, outputDelimiter string) error {
	stats := engine.FetchStatsSnapshot()
	text := generateParagraph(engine, stats, outputDelimiter, false, false)

	if outputFile == "-" {
		fmt.Print(text)
		return nil
	}

	f, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("failed to open output file '%s': %w", outputFile, err)
	}

	if _, err = f.WriteString(text); err != nil {
		return fmt.Errorf("failed to write stats-snapshot to output file '%s': %w", outputFile, err)
	}

	return f.Close()
}

// clearScreen tries to ensure resetting original state of screen
func clearScreen() {
	fmt.Print("\033[2J\033[1;1H\033[?25l")
}

func cleanExit() {
	clearScreen()
	ui.Close()

	// Show cursor once again
	fmt.Print("\033[?25h")
	os.Exit(0)
}

// generateParagraph takes an options map and latest Stats
// then returns a formatted paragraph ready to be rendered
func generateParagraph(engine *Engine, stats *Stats, outputDelimiter string, lookupDNS bool, rawBytes bool) string {
	if len(outputDelimiter) > 0 { //default
		return generateParagraphCSV(engine, stats, outputDelimiter, lookupDNS, rawBytes)
	}

	return generateParagraphPlainText(engine, stats, lookupDNS, rawBytes)
}

const (
	DEFAULT_PADDING_SIZE      = 2
	DEFAULT_PADDING           = "  "
	DEFAULT_HOST_PADDING_SIZE = 15
	UI_HEADER_PREFIX          = "\033[1;1H\033[7;1H"
)

var (
	resolvedHosts = map[string]string{} // cache for reducing DNS lookups in case enabled

	standardHeaders = []interface{}{"SUBS", "PENDING", "MSGS_TO", "MSGS_FROM", "BYTES_TO", "BYTES_FROM", "LANG", "VERSION", "UPTIME", "LAST_ACTIVITY"}

	defaultHeaderColumns = []string{"%-6s", "%-10s", "%-10s", "%-10s", "%-10s", "%-10s", "%-7s", "%-7s", "%-7s", "%-40s"} // Chopped: HOST CID NAME...
	defaultRowColumns    = []string{"%-6d", "%-10s", "%-10s", "%-10s", "%-10s", "%-10s", "%-7s", "%-7s", "%-7s", "%-40s"}
)

func generateParagraphPlainText(engine *Engine, stats *Stats, lookupDNS bool, rawBytes bool) string {
	// Snapshot current stats
	cpu := stats.Varz.CPU
	memVal := stats.Varz.Mem
	uptime := stats.Varz.Uptime
	numConns := stats.Connz.NumConns
	inMsgsVal := stats.Varz.InMsgs
	outMsgsVal := stats.Varz.OutMsgs
	inBytesVal := stats.Varz.InBytes
	outBytesVal := stats.Varz.OutBytes
	slowConsumers := stats.Varz.SlowConsumers
	serverID := stats.Varz.ID

	var serverVersion string
	if stats.Varz.Version != "" {
		serverVersion = stats.Varz.Version
	}
	var serverName string
	if stats.Varz.Name != stats.Varz.ID {
		serverName = stats.Varz.Name
	}

	mem := Psize(false, memVal) //memory is exempt from the rawbytes flag
	inMsgs := Nsize(rawBytes, inMsgsVal)
	outMsgs := Nsize(rawBytes, outMsgsVal)
	inBytes := Psize(rawBytes, inBytesVal)
	outBytes := Psize(rawBytes, outBytesVal)
	inMsgsRate := stats.Rates.InMsgsRate
	outMsgsRate := stats.Rates.OutMsgsRate
	inBytesRate := Psize(rawBytes, int64(stats.Rates.InBytesRate))
	outBytesRate := Psize(rawBytes, int64(stats.Rates.OutBytesRate))

	info := "NATS server version %s (uptime: %s) %s\n"
	info += "Server: %s\n"
	info += "  ID:   %s\n"
	info += "  Load: CPU:  %.1f%%  Memory: %s  Slow Consumers: %d\n"
	info += "  In:   Msgs: %s  Bytes: %s  Msgs/Sec: %.1f  Bytes/Sec: %s\n"
	info += "  Out:  Msgs: %s  Bytes: %s  Msgs/Sec: %.1f  Bytes/Sec: %s"

	text := fmt.Sprintf(
		info, serverVersion, uptime, stats.Error,
		serverName, serverID,
		cpu, mem, slowConsumers,
		inMsgs, inBytes, inMsgsRate, inBytesRate,
		outMsgs, outBytes, outMsgsRate, outBytesRate,
	)

	text += fmt.Sprintf("\n\nConnections Polled: %d\n", numConns)
	displaySubs := engine.DisplaySubs

	header := make([]interface{}, 0) // Dynamically add columns and padding depending
	hostSize := DEFAULT_HOST_PADDING_SIZE

	nameSize := 0 // Disable name unless we have seen one using it
	for _, conn := range stats.Connz.Conns {
		var size int

		var hostname string
		if lookupDNS {
			if addr, present := resolvedHosts[conn.IP]; !present { // Make a lookup for each one of the ips and memoize them for subsequent polls
				addrs, err := net.LookupAddr(conn.IP)
				if err == nil && len(addrs) > 0 && len(addrs[0]) > 0 {
					hostname = addrs[0]
					resolvedHosts[conn.IP] = hostname
				} else {
					// Otherwise just continue to use ip:port as resolved host
					// can be an empty string even though there were no errors
					hostname = fmt.Sprintf("%s:%d", conn.IP, conn.Port)
					resolvedHosts[conn.IP] = hostname
				}
			} else {
				hostname = addr
			}
		} else {
			hostname = fmt.Sprintf("%s:%d", conn.IP, conn.Port)
		}

		size = len(hostname) // host
		if size > hostSize {
			hostSize = size + DEFAULT_PADDING_SIZE
		}

		size = len(conn.Name) // name
		if size > nameSize {
			nameSize = size + DEFAULT_PADDING_SIZE

			minLen := len("NAME") // If using name, ensure that it is not too small...
			if nameSize < minLen {
				nameSize = minLen
			}
		}
	}

	connHeader := DEFAULT_PADDING // Initial padding

	header = append(header, "HOST") // HOST
	connHeader += "%-" + fmt.Sprintf("%d", hostSize) + "s "

	header = append(header, "CID") // CID
	connHeader += " %-6s "

	if nameSize > 0 { // NAME
		header = append(header, "NAME")
		connHeader += "%-" + fmt.Sprintf("%d", nameSize) + "s "
	}

	header = append(header, standardHeaders...)

	connHeader += strings.Join(defaultHeaderColumns, "  ")
	if displaySubs {
		connHeader += "%13s"
	}

	connHeader += "\n" // ...LAST ACTIVITY

	var connRows string
	if displaySubs {
		header = append(header, "SUBSCRIPTIONS")
	}

	connRows = fmt.Sprintf(connHeader, header...)

	text += connRows // Add to screen!

	connValues := DEFAULT_PADDING

	connValues += "%-" + fmt.Sprintf("%d", hostSize) + "s " // HOST: e.g. 192.168.1.1:78901

	connValues += " %-6d " // CID: e.g. 1234

	if nameSize > 0 { // NAME: e.g. hello
		connValues += "%-" + fmt.Sprintf("%d", nameSize) + "s "
	}

	connValues += strings.Join(defaultRowColumns, "  ")
	if displaySubs {
		connValues += "%s"
	}
	connValues += "\n"

	for _, conn := range stats.Connz.Conns {
		var h string
		if lookupDNS {
			if rh, present := resolvedHosts[conn.IP]; present {
				h = rh
			}
		} else {
			h = fmt.Sprintf("%s:%d", conn.IP, conn.Port)
		}

		var connLine string // Build the info line
		connLineInfo := make([]interface{}, 0)
		connLineInfo = append(connLineInfo, h)
		connLineInfo = append(connLineInfo, conn.Cid)

		if nameSize > 0 { // Name not included unless present
			connLineInfo = append(connLineInfo, conn.Name)
		}

		connLineInfo = append(connLineInfo, conn.NumSubs)

		connLineInfo = append(connLineInfo, Nsize(rawBytes, int64(conn.Pending)))

		if !engine.ShowRates {
			connLineInfo = append(connLineInfo, Nsize(rawBytes, conn.OutMsgs), Nsize(rawBytes, conn.InMsgs))
			connLineInfo = append(connLineInfo, Psize(rawBytes, conn.OutBytes), Psize(rawBytes, conn.InBytes))
		} else {
			var (
				inMsgsPerSec   float64
				outMsgsPerSec  float64
				inBytesPerSec  float64
				outBytesPerSec float64
			)
			crate, wasConnected := stats.Rates.Connections[conn.Cid]
			if wasConnected {
				outMsgsPerSec = crate.OutMsgsRate
				inMsgsPerSec = crate.InMsgsRate
				outBytesPerSec = crate.OutBytesRate
				inBytesPerSec = crate.InBytesRate
			}
			connLineInfo = append(connLineInfo, Nsize(rawBytes, int64(outMsgsPerSec)), Nsize(rawBytes, int64(inMsgsPerSec)))
			connLineInfo = append(connLineInfo, Psize(rawBytes, int64(outBytesPerSec)), Psize(rawBytes, int64(inBytesPerSec)))
		}

		connLineInfo = append(connLineInfo, conn.Lang, conn.Version)
		connLineInfo = append(connLineInfo, conn.Uptime, conn.LastActivity)

		if displaySubs {
			subs := strings.Join(conn.Subs, ", ")
			connLineInfo = append(connLineInfo, subs)
		}

		connLine = fmt.Sprintf(connValues, connLineInfo...)

		text += connLine // Add line to screen!
	}

	return text
}

func generateParagraphCSV(engine *Engine, stats *Stats, delimiter string, lookupDNS bool, rawBytes bool) string {
	defaultHeaderAndRowColumnsForCsv := []string{"%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s"} // Chopped: HOST CID NAME...

	cpu := stats.Varz.CPU // Snapshot current stats
	memVal := stats.Varz.Mem
	uptime := stats.Varz.Uptime
	numConns := stats.Connz.NumConns
	inMsgsVal := stats.Varz.InMsgs
	outMsgsVal := stats.Varz.OutMsgs
	inBytesVal := stats.Varz.InBytes
	outBytesVal := stats.Varz.OutBytes
	slowConsumers := stats.Varz.SlowConsumers

	var serverVersion string
	if stats.Varz.Version != "" {
		serverVersion = stats.Varz.Version
	}

	mem := Psize(false, memVal) //memory is exempt from the rawbytes flag
	inMsgs := Nsize(rawBytes, inMsgsVal)
	outMsgs := Nsize(rawBytes, outMsgsVal)
	inBytes := Psize(rawBytes, inBytesVal)
	outBytes := Psize(rawBytes, outBytesVal)
	inMsgsRate := stats.Rates.InMsgsRate
	outMsgsRate := stats.Rates.OutMsgsRate
	inBytesRate := Psize(rawBytes, int64(stats.Rates.InBytesRate))
	outBytesRate := Psize(rawBytes, int64(stats.Rates.OutBytesRate))

	info := "NATS server version[__DELIM__]%s[__DELIM__](uptime: %s)[__DELIM__]%s\n"
	info += "Server:\n"
	info += "Load:[__DELIM__]CPU:[__DELIM__]%.1f%%[__DELIM__]Memory:[__DELIM__]%s[__DELIM__]Slow Consumers:[__DELIM__]%d\n"
	info += "In:[__DELIM__]Msgs:[__DELIM__]%s[__DELIM__]Bytes:[__DELIM__]%s[__DELIM__]Msgs/Sec:[__DELIM__]%.1f[__DELIM__]Bytes/Sec:[__DELIM__]%s\n"
	info += "Out:[__DELIM__]Msgs:[__DELIM__]%s[__DELIM__]Bytes:[__DELIM__]%s[__DELIM__]Msgs/Sec:[__DELIM__]%.1f[__DELIM__]Bytes/Sec:[__DELIM__]%s"

	text := fmt.Sprintf(
		info, serverVersion, uptime, stats.Error,
		cpu, mem, slowConsumers,
		inMsgs, inBytes, inMsgsRate, inBytesRate,
		outMsgs, outBytes, outMsgsRate, outBytesRate,
	)

	text += fmt.Sprintf("\n\nConnections Polled:[__DELIM__]%d\n", numConns)

	displaySubs := engine.DisplaySubs
	for _, conn := range stats.Connz.Conns {
		if !lookupDNS {
			continue
		}

		_, present := resolvedHosts[conn.IP]
		if present {
			continue
		}

		addrs, err := net.LookupAddr(conn.IP)

		hostname := ""
		if err == nil && len(addrs) > 0 && len(addrs[0]) > 0 { // Make a lookup for each one of the ips and memoize them for subsequent polls
			hostname = addrs[0]
		} else { // Otherwise just continue to use ip:port as resolved host can be an empty string even though there were no errors
			hostname = fmt.Sprintf("%s:%d", conn.IP, conn.Port)
		}

		resolvedHosts[conn.IP] = hostname
	}

	header := make([]interface{}, 0) // Dynamically add columns
	connHeader := ""

	header = append(header, "HOST") // HOST
	connHeader += "%s[__DELIM__]"

	header = append(header, "CID") // CID
	connHeader += "%s[__DELIM__]"

	header = append(header, "NAME") // NAME
	connHeader += "%s[__DELIM__]"

	header = append(header, standardHeaders...)
	connHeader += strings.Join(defaultHeaderAndRowColumnsForCsv, "[__DELIM__]")

	if displaySubs {
		connHeader += "[__DELIM__]%s" // SUBSCRIPTIONS
	}

	connHeader += "\n" // ...LAST ACTIVITY

	if displaySubs {
		header = append(header, "SUBSCRIPTIONS")
	}

	text += fmt.Sprintf(connHeader, header...) // Add to screen!

	connValues := "%s[__DELIM__]" // HOST: e.g. 192.168.1.1:78901
	connValues += "%d[__DELIM__]" // CID: e.g. 1234
	connValues += "%s[__DELIM__]" // NAME: e.g. hello

	connValues += strings.Join(defaultHeaderAndRowColumnsForCsv, "[__DELIM__]")
	if displaySubs {
		connValues += "%s"
	}
	connValues += "\n"

	for _, conn := range stats.Connz.Conns {
		var h string
		if lookupDNS {
			if rh, present := resolvedHosts[conn.IP]; present {
				h = rh
			}
		} else {
			h = fmt.Sprintf("%s:%d", conn.IP, conn.Port)
		}

		connLineInfo := make([]interface{}, 0)
		connLineInfo = append(connLineInfo, h)
		connLineInfo = append(connLineInfo, conn.Cid)
		connLineInfo = append(connLineInfo, conn.Name)
		connLineInfo = append(connLineInfo, fmt.Sprintf("%d", conn.NumSubs))
		connLineInfo = append(connLineInfo, Nsize(rawBytes, int64(conn.Pending)), Nsize(rawBytes, conn.OutMsgs), Nsize(rawBytes, conn.InMsgs))
		connLineInfo = append(connLineInfo, Psize(rawBytes, conn.OutBytes), Psize(rawBytes, conn.InBytes))
		connLineInfo = append(connLineInfo, conn.Lang, conn.Version)
		connLineInfo = append(connLineInfo, conn.Uptime, conn.LastActivity)

		if displaySubs {
			subs := "[__DELIM__]" + strings.Join(conn.Subs, "  ") // its safer to use a couple of whitespaces instead of commas to separate the subs because comma is reserved to separate entire columns!
			connLineInfo = append(connLineInfo, subs)
		}

		text += fmt.Sprintf(connValues, connLineInfo...)
	}

	text = strings.ReplaceAll(text, "[__DELIM__]", delimiter)

	return text
}

type ViewMode int

const (
	TopViewMode ViewMode = iota
	HelpViewMode
)

type RedrawCause int

const (
	DueToNewStats RedrawCause = iota
	DueToViewportResize
)

// StartUI periodically refreshes the screen using recent data.
func StartUI(engine *Engine, lookupDNS bool, rawBytes bool, maxRefresh int) {
	cleanStats := &Stats{
		Varz:  &server.Varz{},
		Connz: &server.Connz{},
		Rates: &Rates{},
		Error: fmt.Errorf(""),
	}

	// Show empty values on first display
	text := generateParagraph(engine, cleanStats, "", lookupDNS, rawBytes)
	par := ui.NewPar(text)
	par.Height = ui.TermHeight()
	par.Width = ui.TermWidth()
	par.HasBorder = false

	helpText := generateHelp()
	helpPar := ui.NewPar(helpText)
	helpPar.Height = ui.TermHeight()
	helpPar.Width = ui.TermWidth()
	helpPar.HasBorder = false

	// Top like view
	paraRow := ui.NewRow(ui.NewCol(ui.TermWidth(), 0, par))

	// Help view
	helpParaRow := ui.NewRow(ui.NewCol(ui.TermWidth(), 0, helpPar))

	// Create grids that we'll be using to toggle what to render
	topViewGrid := ui.NewGrid(paraRow)
	helpViewGrid := ui.NewGrid(helpParaRow)

	// Start with the topviewGrid by default
	ui.Body.Rows = topViewGrid.Rows
	ui.Body.Align()

	// Used to toggle back to previous mode
	viewMode := TopViewMode

	// Used for pinging the IU to refresh the screen with new values
	redraw := make(chan RedrawCause)

	update := func() {
		for {
			stats := <-engine.StatsCh

			par.Text = generateParagraph(engine, stats, "", lookupDNS, rawBytes) // Update top view text

			redraw <- DueToNewStats
		}
	}

	// Flags for capturing options
	waitingSortOption := false
	waitingLimitOption := false

	optionBuf := ""
	refreshOptionHeader := func() {
		clrline := fmt.Sprintf("%s                  ", UI_HEADER_PREFIX) // Need to mask what was typed before

		clrline += "  "
		for i := 0; i < len(optionBuf); i++ {
			clrline += "  "
		}
		fmt.Print(clrline)
	}

	evt := ui.EventCh()

	ui.Render(ui.Body)

	go update()

	numberOfRedrawsDueToNewStats := 0
	for {
		select {
		case e := <-evt:

			if waitingSortOption {

				if e.Type == ui.EventKey && e.Key == ui.KeyEnter {

					sortOpt := server.SortOpt(optionBuf)
					if sortOpt.IsValid() {
						engine.SortOpt = sortOpt
					} else {
						go func() {
							// Has to be at least of the same length as sort by header
							emptyPadding := "       "
							fmt.Printf("%sinvalid order: %s%s", UI_HEADER_PREFIX, optionBuf, emptyPadding)
							waitingSortOption = false
							time.Sleep(1 * time.Second)
							refreshOptionHeader()
							optionBuf = ""
						}()
						continue
					}

					refreshOptionHeader()
					waitingSortOption = false
					optionBuf = ""
					continue
				}

				// Handle backspace
				if e.Type == ui.EventKey && len(optionBuf) > 0 && (e.Key == ui.KeyBackspace || e.Key == ui.KeyBackspace2) {
					optionBuf = optionBuf[:len(optionBuf)-1]
					refreshOptionHeader()
				} else {
					optionBuf += string(e.Ch)
				}
				fmt.Printf("%ssort by [%s]: %s", UI_HEADER_PREFIX, engine.SortOpt, optionBuf)
			}

			if waitingLimitOption {

				if e.Type == ui.EventKey && e.Key == ui.KeyEnter {

					var n int
					_, err := fmt.Sscanf(optionBuf, "%d", &n)
					if err == nil {
						engine.Conns = n
					}

					waitingLimitOption = false
					optionBuf = ""
					refreshOptionHeader()
					continue
				}

				// Handle backspace
				if e.Type == ui.EventKey && len(optionBuf) > 0 && (e.Key == ui.KeyBackspace || e.Key == ui.KeyBackspace2) {
					optionBuf = optionBuf[:len(optionBuf)-1]
					refreshOptionHeader()
				} else {
					optionBuf += string(e.Ch)
				}
				fmt.Printf("%slimit   [%d]: %s", UI_HEADER_PREFIX, engine.Conns, optionBuf)
			}

			if e.Type == ui.EventKey && e.Key == ui.KeySpace {
				engine.ShowRates = !engine.ShowRates
			}

			if e.Type == ui.EventKey && (e.Ch == 'q' || e.Key == ui.KeyCtrlC) {
				close(engine.ShutdownCh)
				cleanExit()
			}

			if e.Type == ui.EventKey && e.Ch == 's' && !(waitingLimitOption || waitingSortOption) {
				engine.DisplaySubs = !engine.DisplaySubs
			}

			if e.Type == ui.EventKey && viewMode == HelpViewMode {
				ui.Body.Rows = topViewGrid.Rows
				viewMode = TopViewMode
				continue
			}

			if e.Type == ui.EventKey && e.Ch == 'o' && !waitingLimitOption && viewMode == TopViewMode {
				fmt.Printf("%ssort by [%s]:", UI_HEADER_PREFIX, engine.SortOpt)
				waitingSortOption = true
			}

			if e.Type == ui.EventKey && e.Ch == 'n' && !waitingSortOption && viewMode == TopViewMode {
				fmt.Printf("%slimit   [%d]:", UI_HEADER_PREFIX, engine.Conns)
				waitingLimitOption = true
			}

			if e.Type == ui.EventKey && (e.Ch == '?' || e.Ch == 'h') && !(waitingSortOption || waitingLimitOption) {
				if viewMode == TopViewMode {
					refreshOptionHeader()
					optionBuf = ""
				}

				ui.Body.Rows = helpViewGrid.Rows
				viewMode = HelpViewMode
				waitingLimitOption = false
				waitingSortOption = false
			}

			if e.Type == ui.EventKey && (e.Ch == 'd') && !(waitingSortOption || waitingLimitOption) {
				lookupDNS = !lookupDNS
			}

			if e.Type == ui.EventKey && (e.Ch == 'b') && !(waitingSortOption || waitingLimitOption) {
				rawBytes = !rawBytes
			}

			if e.Type == ui.EventResize {
				ui.Body.Width = ui.TermWidth()
				ui.Body.Align()
				go func() { redraw <- DueToViewportResize }()
			}

		case cause := <-redraw:
			ui.Render(ui.Body)

			if cause == DueToNewStats {
				numberOfRedrawsDueToNewStats += 1

				if maxRefresh > 0 && numberOfRedrawsDueToNewStats >= maxRefresh {
					close(engine.ShutdownCh)
					cleanExit()
				}
			}
		}
	}
}

func generateHelp() string {
	text := `
Command          Description

o<option>        Set primary sort key to <option>.

                 Option can be one of: {cid|subs|pending|msgs_to|msgs_from|
                 bytes_to|bytes_from|idle|last}

                 This can be set in the command line too with -sort flag.

n<limit>         Set sample size of connections to request from the server.

                 This can be set in the command line as well via -n flag.
                 Note that if used in conjunction with sort, the server
                 would respect both options allowing queries like 'connection
                 with largest number of subscriptions': -n 1 -sort subs

s                Toggle displaying connection subscriptions.

d                Toggle activating DNS address lookup for clients.

b                Toggle displaying raw bytes.

space            Toggle displaying rates per second in connections.

q                Quit nats-top.

Press any key to continue...

`
	return text
}
