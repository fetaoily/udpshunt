package tui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/fetaoily/udpshunt/internal/admin"
)

// httpClient bounds each status poll: the TUI polls on a single
// fetch -> status -> tick chain, so a hung request without a deadline would
// freeze the dashboard forever.
var httpClient = &http.Client{Timeout: 5 * time.Second}

// Run starts the dashboard against the admin API at addr, polling every
// interval. It blocks until the user quits (q / Ctrl-C).
func Run(addr string, interval time.Duration) error {
	m := model{addr: addr, interval: interval}
	_, err := tea.NewProgram(m, tea.WithAltScreen()).Run()
	return err
}

type statusMsg struct {
	st  admin.Status
	err error
}

type clientsMsg struct {
	cs  admin.ClientsStatus
	err error
}

type tickMsg struct{}

type model struct {
	addr     string
	interval time.Duration

	status  *admin.Status
	clients *admin.ClientsStatus
	errText string

	// Clients view state: view is "dash" or "clients"; sortIdx indexes
	// clientCols; sortAsc flips the /clients order parameter; clientsBlocked
	// switches the /clients fetch to the blocked-traffic table.
	view           string
	sortIdx        int
	sortAsc        bool
	clientsBlocked bool
}

func (m model) Init() tea.Cmd {
	// Start with one fetch; statusMsg re-arms the tick, forming a single
	// fetch -> status -> tick -> fetch chain (no duplicate polling chains).
	return fetchCmd(m)
}

func tickCmd(interval time.Duration) tea.Cmd {
	return tea.Tick(interval, func(time.Time) tea.Msg { return tickMsg{} })
}

func fetchCmd(m model) tea.Cmd {
	return func() tea.Msg {
		st, err := fetchStatus(m.addr)
		return statusMsg{st: st, err: err}
	}
}

func fetchStatus(addr string) (admin.Status, error) {
	var st admin.Status
	resp, err := httpClient.Get(strings.TrimRight(addr, "/") + "/status")
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status endpoint returned %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// fetchClients reads the top-100 client rows for the current sort column;
// blocked selects the blocked-traffic table instead of tracked clients.
func fetchClients(addr, sortCol string, asc, blocked bool) (admin.ClientsStatus, error) {
	var cs admin.ClientsStatus
	u := strings.TrimRight(addr, "/") + "/clients?sort=" + url.QueryEscape(sortCol) + "&limit=100"
	if asc {
		u += "&order=asc"
	}
	if blocked {
		u += "&scope=blocked"
	}
	resp, err := httpClient.Get(u)
	if err != nil {
		return cs, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cs, fmt.Errorf("clients endpoint returned %d", resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&cs)
	return cs, err
}

func fetchClientsCmd(m model) tea.Cmd {
	sortCol := clientCols[m.sortIdx].sort
	return func() tea.Msg {
		cs, err := fetchClients(m.addr, sortCol, m.sortAsc, m.clientsBlocked)
		return clientsMsg{cs: cs, err: err}
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "c":
			if m.view == "clients" {
				m.view = "dash"
				return m, fetchCmd(m) // the dash view needs a fresh status now
			}
			m.view = "clients"
			return m, fetchClientsCmd(m)
		case "s":
			if m.view == "clients" {
				m.sortIdx = (m.sortIdx + 1) % len(clientCols)
				return m, fetchClientsCmd(m)
			}
		case "r":
			if m.view == "clients" {
				m.sortAsc = !m.sortAsc
				return m, fetchClientsCmd(m)
			}
		case "b":
			if m.view == "clients" {
				m.clientsBlocked = !m.clientsBlocked
				return m, fetchClientsCmd(m)
			}
		}
	case tea.WindowSizeMsg:
		return m, nil
	case statusMsg:
		if msg.err != nil {
			m.errText = msg.err.Error()
		} else {
			m.status = &msg.st
			m.errText = ""
		}
		return m, tickCmd(m.interval)
	case clientsMsg:
		if msg.err != nil {
			m.errText = msg.err.Error()
		} else {
			m.clients = &msg.cs
			m.errText = ""
		}
		return m, tickCmd(m.interval)
	case tickMsg:
		// The chain follows the active view; a stale message from the other
		// view just updates its snapshot harmlessly.
		if m.view == "clients" {
			return m, fetchClientsCmd(m)
		}
		return m, fetchCmd(m)
	}
	return m, nil
}

func (m model) View() string {
	if m.view == "clients" {
		return m.viewClients()
	}
	var b strings.Builder
	b.WriteString(titleStyle.Render("udpshunt") + dimStyle.Render("  "+m.addr))
	if m.errText != "" {
		b.WriteString("  " + downStyle.Render("ERR: "+m.errText))
	}
	b.WriteString("\n\n")

	if m.status == nil {
		b.WriteString(dimStyle.Render("waiting for first sample...\n"))
		return b.String()
	}
	st := m.status

	tv := totalsFromStatus(st, m.interval)

	healthy, total := uniqueBackendHealth(st)
	blocked := ""
	if st.Blacklist != nil && st.Blacklist.BlockedPackets > 0 {
		blocked = fmt.Sprintf("   blocked %d", st.Blacklist.BlockedPackets)
	}
	fmt.Fprintf(&b, "uptime %s   sessions %d   backends %d/%d up   in %s/s (%.0f pps)   out %s/s (%.0f pps)%s\n\n",
		st.Uptime, st.Sessions.Active, healthy, total,
		humanBytes(tv.InBPS), tv.InPPS, humanBytes(tv.OutBPS), tv.OutPPS, blocked)

	for _, row := range listenerRows(historyMap(st), st.Listeners, 40) {
		fmt.Fprintf(&b, "%s  %s  %s  sessions %d  backends %d/%d\n",
			titleStyle.Render(row.Name), dimStyle.Render(row.Bind), dimStyle.Render(row.Balance),
			row.Sessions, row.Healthy, row.Backends)
		fmt.Fprintf(&b, "  in  %s\n  out %s\n", row.InLine, row.OutLine)
		for _, be := range backendsOf(st, row.Name) {
			marker := healthText(be.Healthy)
			fmt.Fprintf(&b, "    %s %s  sessions %d\n", marker, be.Addr, be.Sessions)
		}
		b.WriteString("\n")
	}

	b.WriteString(dimStyle.Render("events:\n"))
	events := st.Events
	if len(events) > 5 {
		events = events[len(events)-5:]
	}
	for _, e := range events {
		fmt.Fprintf(&b, "  %s %s %s\n", e.Time.Format("15:04:05"), e.Kind, e.Detail)
	}
	b.WriteString(dimStyle.Render("\nc: clients  q: quit"))
	return b.String()
}

// viewClients renders the per-client-IP table (htop-style: click-free sort
// via s / r keys).
func (m model) viewClients() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("udpshunt clients") + dimStyle.Render("  "+m.addr))
	if m.errText != "" {
		b.WriteString("  " + downStyle.Render("ERR: "+m.errText))
	}
	b.WriteString("\n\n")
	if m.clients == nil {
		b.WriteString(dimStyle.Render("waiting for first sample...\n"))
		return b.String()
	}
	cs := m.clients
	if !cs.Enabled {
		b.WriteString("client stats are disabled (client_stats.enabled: false in the config)\n")
		b.WriteString(dimStyle.Render("\nc: dashboard  q: quit"))
		return b.String()
	}
	col := clientCols[m.sortIdx]
	dir := "desc"
	if m.sortAsc {
		dir = "asc"
	}
	scope := ""
	if m.clientsBlocked {
		scope = "   blocked"
	}
	fmt.Fprintf(&b, "tracked %d   evicted %d   sort %s %s%s\n\n", cs.Tracked, cs.Evicted, col.sort, dir, scope)
	const maxRows = 20
	rows := cs.Rows
	if len(rows) > maxRows {
		rows = rows[:maxRows]
	}
	now := time.Now()
	w := clientWidths(rows, m.sortIdx, m.sortAsc, now)
	b.WriteString(clientHeader(w, m.sortIdx, m.sortAsc) + "\n")
	for _, r := range rows {
		b.WriteString(clientRow(r, w, now) + "\n")
	}
	if len(cs.Rows) > maxRows {
		fmt.Fprintf(&b, dimStyle.Render("  ... %d more rows below\n"), len(cs.Rows)-maxRows)
	}
	b.WriteString(dimStyle.Render("\nc: dashboard  s: sort column  r: reverse  b: blocked  q: quit"))
	return b.String()
}

// uniqueBackendHealth counts DISTINCT backend addresses across listeners:
// the same address probed by several listeners (e.g. a zero-traffic watchdog
// alongside the production listener) is one backend, not two. It counts as up
// when any listener reports it healthy — per-listener disagreement stays
// visible in each listener's own row.
func uniqueBackendHealth(st *admin.Status) (healthy, total int) {
	up := make(map[string]bool)
	for _, ls := range st.Listeners {
		for _, be := range ls.Backends {
			if h, seen := up[be.Addr]; seen {
				up[be.Addr] = h || be.Healthy
				continue
			}
			up[be.Addr] = be.Healthy
		}
	}
	for _, h := range up {
		total++
		if h {
			healthy++
		}
	}
	return healthy, total
}

func backendsOf(st *admin.Status, name string) []admin.BackendStatus {
	for _, ls := range st.Listeners {
		if ls.Name == name {
			return ls.Backends
		}
	}
	return nil
}

func historyMap(st *admin.Status) map[string][]admin.Sample {
	m := map[string][]admin.Sample{}
	for _, ls := range st.Listeners {
		m[ls.Name] = ls.History
	}
	return m
}

// totalsFromStatus computes daemon-wide rates from each listener's ring.
func totalsFromStatus(st *admin.Status, interval time.Duration) totalView {
	hm := historyMap(st)
	prev := map[string][]admin.Sample{}
	cur := map[string][]admin.Sample{}
	for name, samples := range hm {
		if len(samples) >= 2 {
			prev[name] = samples[:len(samples)-1]
			cur[name] = samples
		} else if len(samples) == 1 {
			cur[name] = samples
		}
	}
	return totals(prev, cur, interval)
}
