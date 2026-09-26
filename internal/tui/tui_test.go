package tui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/fetaoily/udpshunt/internal/admin"
	"github.com/fetaoily/udpshunt/internal/clientstats"
)

func TestSparklineShapes(t *testing.T) {
	if got := sparkline(nil, 10); got != "" {
		t.Fatalf("empty input = %q", got)
	}
	s := sparkline([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8}, 9)
	if len([]rune(s)) != 9 {
		t.Fatalf("width = %q", s)
	}
	if s == sparkline([]float64{8, 7, 6, 5, 4, 3, 2, 1, 0}, 9) {
		t.Fatal("rising and falling series must render differently")
	}
	constant := sparkline([]float64{5, 5, 5, 5}, 4)
	if constant == "" {
		t.Fatal("constant series must still render")
	}
	// Width capping keeps only the last `width` values.
	capped := sparkline([]float64{0, 1, 2, 3, 4, 5, 6, 7, 8}, 5)
	if len([]rune(capped)) != 5 {
		t.Fatalf("capped width = %q", capped)
	}
}

func TestHealthText(t *testing.T) {
	up, down := healthText(true), healthText(false)
	if up == down {
		t.Fatal("up and down must render differently")
	}
	// Plain-text content (color codes aside) must identify the state.
	if !contains(up, "UP") || !contains(down, "DOWN") {
		t.Fatalf("up=%q down=%q", up, down)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func TestTotalsSumsListeners(t *testing.T) {
	prev := map[string][]admin.Sample{
		"a": {{T: 1000, In: 10, Out: 5, BytesIn: 100, BytesOut: 50}},
		"b": {{T: 1000, In: 1, Out: 1, BytesIn: 10, BytesOut: 10}},
	}
	cur := map[string][]admin.Sample{
		"a": {{T: 2000, In: 20, Out: 15, BytesIn: 200, BytesOut: 150}},
		"b": {{T: 2000, In: 3, Out: 2, BytesIn: 30, BytesOut: 20}},
	}
	tv := totals(prev, cur, time.Second)
	if tv.InPPS != 12 || tv.OutPPS != 11 {
		t.Fatalf("pps = %+v", tv)
	}
	if tv.InBPS != 120 || tv.OutBPS != 110 {
		t.Fatalf("bps = %+v", tv)
	}
}

func TestFetchStatus(t *testing.T) {
	want := admin.Status{Uptime: "1s"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer ts.Close()
	got, err := fetchStatus(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got.Uptime != "1s" {
		t.Fatalf("uptime = %q", got.Uptime)
	}
}

func TestFetchStatusError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	if _, err := fetchStatus(ts.URL); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestFetchClients(t *testing.T) {
	var gotSort, gotOrder, gotLimit, gotScope string
	want := admin.ClientsStatus{
		Enabled: true,
		Tracked: 1,
		Rows: []clientstats.Row{
			{IP: "203.0.113.7", Requests: 3},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		gotSort, gotOrder, gotLimit, gotScope = q.Get("sort"), q.Get("order"), q.Get("limit"), q.Get("scope")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer ts.Close()

	got, err := fetchClients(ts.URL, "requests", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if gotSort != "requests" || gotOrder != "" || gotLimit != "100" || gotScope != "" {
		t.Fatalf("query = sort=%q order=%q limit=%q scope=%q", gotSort, gotOrder, gotLimit, gotScope)
	}
	if len(got.Rows) != 1 || got.Rows[0].IP != "203.0.113.7" {
		t.Fatalf("decoded = %+v", got)
	}

	if _, err := fetchClients(ts.URL, "bytes_in", true, false); err != nil {
		t.Fatal(err)
	}
	if gotSort != "bytes_in" || gotOrder != "asc" {
		t.Fatalf("asc query = sort=%q order=%q", gotSort, gotOrder)
	}

	// blocked=true selects the blocked-traffic table via scope=blocked.
	if _, err := fetchClients(ts.URL, "ip", false, true); err != nil {
		t.Fatal(err)
	}
	if gotScope != "blocked" {
		t.Fatalf("blocked scope = %q, want blocked", gotScope)
	}
}

func TestClientsViewRendersTable(t *testing.T) {
	m := model{
		view: "clients",
		clients: &admin.ClientsStatus{
			Enabled: true,
			Tracked: 2,
			Evicted: 1,
			Rows: []clientstats.Row{
				{IP: "203.0.113.7", Requests: 12, Responses: 10, BytesIn: 1200, BytesOut: 9800, PPSIn: 3, LastSeen: time.Now().Add(-2 * time.Second).UnixNano()},
			},
		},
	}
	v := m.View()
	for _, want := range []string{"203.0.113.7", "tracked 2", "evicted 1", "s: sort column", "r: reverse"} {
		if !contains(v, want) {
			t.Fatalf("clients view missing %q:\n%s", want, v)
		}
	}
}

func TestClientsViewDisabled(t *testing.T) {
	m := model{view: "clients", clients: &admin.ClientsStatus{Enabled: false}}
	v := m.View()
	if !contains(v, "disabled") {
		t.Fatalf("disabled view must say so:\n%s", v)
	}
}

func TestClientsKeyHandling(t *testing.T) {
	m := model{addr: "http://x", interval: time.Second}
	key := func(s string) tea.KeyMsg {
		if s == "ctrl+c" {
			return tea.KeyMsg{Type: tea.KeyCtrlC}
		}
		return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)}
	}
	step := func(cur model, k string) model {
		next, _ := cur.Update(key(k))
		return next.(model)
	}

	// c enters the clients view and issues a clients fetch.
	m2, cmd := m.Update(key("c"))
	if m2.(model).view != "clients" || cmd == nil {
		t.Fatalf("c from dash: view=%q cmd=%v", m2.(model).view, cmd)
	}
	if _, ok := cmd().(clientsMsg); !ok {
		t.Fatal("c must fetch clients immediately")
	}

	// s cycles the sort column and wraps after a full cycle.
	m3 := m2.(model)
	for i := 0; i < len(clientCols); i++ {
		m3 = step(m3, "s")
	}
	if m3.sortIdx != m2.(model).sortIdx {
		t.Fatalf("sortIdx=%d after full cycle, want wrap to %d", m3.sortIdx, m2.(model).sortIdx)
	}

	// r flips the direction.
	asc := m3.sortAsc
	m4 := step(m3, "r")
	if m4.sortAsc == asc {
		t.Fatal("r must flip sortAsc")
	}

	// c returns to the dashboard.
	m5 := step(m4, "c")
	if m5.view != "dash" {
		t.Fatalf("c from clients: view=%q", m5.view)
	}

	// q quits.
	if _, cmd := m5.Update(key("q")); cmd == nil {
		t.Fatal("q must quit")
	}
}

func TestClientsViewBlockedMarker(t *testing.T) {
	sample := func(blocked bool) model {
		return model{
			view:           "clients",
			clientsBlocked: blocked,
			clients: &admin.ClientsStatus{
				Enabled: true,
				Tracked: 2,
				Evicted: 1,
				Rows: []clientstats.Row{
					{IP: "203.0.113.7", Requests: 12},
				},
			},
		}
	}
	// The footer hint mentions b: blocked in both scopes, so inspect the
	// tracked/evicted header line itself.
	headerLine := func(v string) string {
		for _, line := range strings.Split(v, "\n") {
			if strings.HasPrefix(line, "tracked ") {
				return line
			}
		}
		return ""
	}
	if h := headerLine(sample(true).View()); !contains(h, "blocked") {
		t.Fatalf("blocked scope must mark the header line: %q", h)
	}
	if h := headerLine(sample(false).View()); contains(h, "blocked") {
		t.Fatalf("normal scope must not mark the header line: %q", h)
	}
}

func TestClientsKeyBlockedToggle(t *testing.T) {
	key := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b")}

	// b outside the clients view does nothing.
	dash := model{addr: "http://x", interval: time.Second}
	next, cmd := dash.Update(key)
	if next.(model).clientsBlocked || cmd != nil {
		t.Fatal("b must be clients-view only")
	}

	// b in the clients view enters the blocked scope and refetches.
	c := dash
	c.view = "clients"
	next, cmd = c.Update(key)
	m2 := next.(model)
	if !m2.clientsBlocked {
		t.Fatal("b must enter blocked scope")
	}
	if _, ok := cmd().(clientsMsg); !ok {
		t.Fatal("b must refetch clients")
	}

	// b again returns to the normal table.
	next, _ = m2.Update(key)
	if next.(model).clientsBlocked {
		t.Fatal("b again must leave blocked scope")
	}
}

func TestDashboardBlockedCount(t *testing.T) {
	m := model{status: &admin.Status{
		Uptime:    "1s",
		Blacklist: &admin.BlacklistStatus{BlockedPackets: 5},
	}}
	if v := m.View(); !contains(v, "blocked 5") {
		t.Fatalf("dashboard must show the blocked count:\n%s", v)
	}

	// No blacklist block (or a zero count) means no blocked figure.
	for _, st := range []*admin.Status{
		{Uptime: "1s"},
		{Uptime: "1s", Blacklist: &admin.BlacklistStatus{}},
	} {
		if v := (model{status: st}).View(); contains(v, "blocked") {
			t.Fatalf("status without dropped packets must not show blocked:\n%s", v)
		}
	}
}

func TestDashboardIllegalCount(t *testing.T) {
	sample := func(illegal int64) model {
		return model{status: &admin.Status{
			Uptime:         "1s",
			IllegalPackets: illegal,
		}}
	}
	// The header line is the one starting with "uptime"; isolating it keeps
	// the assertion on the dashboard header segment itself.
	headerLine := func(v string) string {
		for _, line := range strings.Split(v, "\n") {
			if strings.HasPrefix(line, "uptime ") {
				return line
			}
		}
		return ""
	}
	if h := headerLine(sample(3).View()); !contains(h, "illegal 3") {
		t.Fatalf("illegal drops must mark the header line: %q", h)
	}
	if h := headerLine(sample(0).View()); contains(h, "illegal") {
		t.Fatalf("status without illegal drops must not show illegal: %q", h)
	}
}

func TestUniqueBackendHealth(t *testing.T) {
	st := admin.Status{Listeners: []admin.ListenerStatus{
		{Name: "watchdog", Backends: []admin.BackendStatus{
			{Addr: "10.0.0.1:1", Healthy: false},
			{Addr: "10.0.0.2:1", Healthy: true},
		}},
		{Name: "prod", Backends: []admin.BackendStatus{
			{Addr: "10.0.0.1:1", Healthy: true}, // same addr as above: one backend, any-healthy
			{Addr: "10.0.0.3:1", Healthy: false},
		}},
	}}
	healthy, total := uniqueBackendHealth(&st)
	if total != 3 {
		t.Fatalf("total = %d, want 3 (distinct addresses)", total)
	}
	if healthy != 2 {
		t.Fatalf("healthy = %d, want 2 (10.0.0.1 up in prod, 10.0.0.2 up)", healthy)
	}
}
