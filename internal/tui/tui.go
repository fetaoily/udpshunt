package tui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/fetaoily/udpshunt/internal/admin"
)

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

type tickMsg struct{}

type model struct {
	addr     string
	interval time.Duration

	status  *admin.Status
	errText string
}

func (m model) Init() tea.Cmd {
	return tea.Batch(fetchCmd(m), tickCmd(m.interval))
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
	resp, err := http.Get(strings.TrimRight(addr, "/") + "/status")
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

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if s := msg.String(); s == "q" || s == "ctrl+c" {
			return m, tea.Quit
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
	case tickMsg:
		return m, fetchCmd(m)
	}
	return m, nil
}

func (m model) View() string {
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

	healthy, total := 0, 0
	for _, ls := range st.Listeners {
		for _, be := range ls.Backends {
			total++
			if be.Healthy {
				healthy++
			}
		}
	}
	fmt.Fprintf(&b, "uptime %s   sessions %d   backends %d/%d up   in %s/s (%.0f pps)   out %s/s (%.0f pps)\n\n",
		st.Uptime, st.Sessions.Active, healthy, total,
		humanBytes(tv.InBPS), tv.InPPS, humanBytes(tv.OutBPS), tv.OutPPS)

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
	b.WriteString(dimStyle.Render("\nq: quit"))
	return b.String()
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
