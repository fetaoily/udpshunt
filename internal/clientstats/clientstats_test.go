package clientstats

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manual clock so tests drive ticks and days deterministically.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t *testing.T) *fakeClock {
	t.Helper()
	c, err := time.ParseInLocation(dayFormat+" 15:04:05", "2026-09-18 10:00:00", time.Local)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeClock{now: c}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newTestTable builds a table whose background ticker never fires (1h), so
// tests drive onTick manually for deterministic rates, eviction and rollover.
func newTestTable(t *testing.T, c *fakeClock, mutate func(*Options)) *Table {
	t.Helper()
	opts := Options{
		Dir:              t.TempDir(),
		SnapshotInterval: 30 * time.Second,
		TickEvery:        time.Hour,
		Clock:            c.Now,
	}
	if mutate != nil {
		mutate(&opts)
	}
	tab := New(opts)
	t.Cleanup(func() { tab.Stop() })
	return tab
}

func ip(t *testing.T, s string) net.IP {
	t.Helper()
	return net.ParseIP(s)
}

func waitFor(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// waitForAbsent polls until path is gone; used to prove an in-flight gzip
// finished (the plain file is removed only after the .gz is complete).
func waitForAbsent(t *testing.T, timeout time.Duration, path string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("file still present: %s", path)
}

func TestRecordAndTop(t *testing.T) {
	c := newFakeClock(t)
	tab := newTestTable(t, c, nil)

	tab.PacketIn(ip(t, "10.0.0.1"), 100)
	tab.PacketIn(ip(t, "10.0.0.1"), 50)
	tab.PacketIn(ip(t, "10.0.0.2"), 10)
	tab.PacketOut(ip(t, "10.0.0.1"), 500)
	tab.PacketIn(nil, 5) // nil IP: ignored, no panic

	rows, tracked, evicted := tab.Top("requests", false, 0)
	if tracked != 2 || evicted != 0 {
		t.Fatalf("tracked=%d evicted=%d, want 2/0", tracked, evicted)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// Descending by requests: .1 has 2 requests, .2 has 1.
	if rows[0].IP != "10.0.0.1" || rows[0].Requests != 2 {
		t.Fatalf("first row %+v, want 10.0.0.1 with 2 requests", rows[0])
	}
	if rows[0].BytesIn != 150 || rows[0].BytesOut != 500 || rows[0].Responses != 1 {
		t.Fatalf("row .1 counters wrong: %+v", rows[0])
	}
	if rows[1].IP != "10.0.0.2" || rows[1].Requests != 1 || rows[1].BytesIn != 10 {
		t.Fatalf("row .2 wrong: %+v", rows[1])
	}

	// Ascending flips the order.
	asc, _, _ := tab.Top("requests", true, 0)
	if asc[0].IP != "10.0.0.2" {
		t.Fatalf("asc first row %s, want 10.0.0.2", asc[0].IP)
	}

	// Unknown column falls back to requests (desc).
	fallback, _, _ := tab.Top("nonsense", false, 0)
	if fallback[0].IP != "10.0.0.1" {
		t.Fatalf("unknown column fallback row %s, want 10.0.0.1", fallback[0].IP)
	}

	// Limit truncates.
	limited, _, _ := tab.Top("requests", false, 1)
	if len(limited) != 1 || limited[0].IP != "10.0.0.1" {
		t.Fatalf("limit=1 got %+v", limited)
	}

	// Sorting by bytes_out puts .1 first either way.
	bo, _, _ := tab.Top("bytes_out", false, 0)
	if bo[0].IP != "10.0.0.1" || bo[1].IP != "10.0.0.2" || bo[1].BytesOut != 0 {
		t.Fatalf("bytes_out sort wrong: %+v", bo)
	}
}

func TestTopTieBreakStable(t *testing.T) {
	c := newFakeClock(t)
	tab := newTestTable(t, c, nil)
	for _, ipp := range []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"} {
		tab.PacketIn(ip(t, ipp), 1)
	}
	rows, _, _ := tab.Top("bytes_in", false, 0) // all tied: IP order must hold
	want := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	for i, w := range want {
		if rows[i].IP != w {
			t.Fatalf("row %d = %s, want %s (ties must be IP-ordered)", i, rows[i].IP, w)
		}
	}
}

func TestEvictionLeastRecentlyActive(t *testing.T) {
	c := newFakeClock(t)
	tab := newTestTable(t, c, func(o *Options) { o.MaxIPs = 2 })

	tab.PacketIn(ip(t, "10.0.0.1"), 1) // oldest
	c.Add(time.Second)
	tab.PacketIn(ip(t, "10.0.0.2"), 1)
	c.Add(time.Second)
	tab.PacketIn(ip(t, "10.0.0.3"), 1) // newest
	if tab.Tracked() != 3 {
		t.Fatalf("tracked=%d before eviction, want 3", tab.Tracked())
	}
	c.Add(time.Second)
	tab.onTick(c.Now()) // cap 2: evict .1 (oldest lastSeen)

	if tab.Tracked() != 2 {
		t.Fatalf("tracked=%d after eviction, want 2", tab.Tracked())
	}
	if tab.Evicted() != 1 {
		t.Fatalf("evicted=%d, want 1", tab.Evicted())
	}
	rows, _, _ := tab.Top("requests", false, 0)
	for _, r := range rows {
		if r.IP == "10.0.0.1" {
			t.Fatalf("10.0.0.1 should have been evicted, got rows %+v", rows)
		}
	}
}

func TestHardCapOnInsert(t *testing.T) {
	c := newFakeClock(t)
	tab := newTestTable(t, c, func(o *Options) { o.MaxIPs = 2 }) // hard cap 4

	for i := 1; i <= 4; i++ {
		tab.PacketIn(ip(t, "10.0.0."+string(rune('0'+i))), 1)
	}
	// No tick has run, so soft eviction has not happened; a 5th distinct IP
	// hits the hard cap and goes uncounted.
	tab.PacketIn(ip(t, "10.0.0.9"), 1)
	if got := tab.Tracked(); got != 4 {
		t.Fatalf("tracked=%d, want 4 (hard cap)", got)
	}
	rows, _, _ := tab.Top("requests", false, 0)
	for _, r := range rows {
		if r.IP == "10.0.0.9" {
			t.Fatalf("10.0.0.9 must not be tracked past the hard cap: %+v", rows)
		}
	}
}

func TestRatesEWMASmoothing(t *testing.T) {
	c := newFakeClock(t)
	tab := newTestTable(t, c, nil)

	tab.onTick(c.Now()) // dt == 0 from New: no-op, no panic

	// 10 packets in the first rate window (2s), then tick.
	for i := 0; i < 10; i++ {
		tab.PacketIn(ip(t, "10.0.0.1"), 20)
	}
	c.Add(2 * time.Second)
	tab.onTick(c.Now()) // baseline: prev counters snapshotted, no rate yet

	rows, _, _ := tab.Top("requests", false, 0)
	if rows[0].PPSIn != 0 {
		t.Fatalf("pps after baseline tick = %v, want 0", rows[0].PPSIn)
	}

	// 10 more packets over the next 2s: dr = 5 pps, EWMA(0.5) -> 2.5.
	for i := 0; i < 10; i++ {
		tab.PacketIn(ip(t, "10.0.0.1"), 20)
	}
	c.Add(2 * time.Second)
	tab.onTick(c.Now())

	rows, _, _ = tab.Top("requests", false, 0)
	if rows[0].PPSIn != 2.5 {
		t.Fatalf("pps = %v, want 2.5", rows[0].PPSIn)
	}
	// bytes: 10*20=200 over 2s -> dbi=100, EWMA -> 50.
	if rows[0].BPSIn != 50 {
		t.Fatalf("bps_in = %v, want 50", rows[0].BPSIn)
	}
}

func TestRolloverFinalizesResetsAndRecovers(t *testing.T) {
	c := newFakeClock(t)
	var dir string
	tab := newTestTable(t, c, func(o *Options) { dir = o.Dir })

	tab.PacketIn(ip(t, "10.0.0.1"), 10)
	tab.PacketIn(ip(t, "10.0.0.1"), 5)

	// Cross midnight: the old day's file is finalized, gzipped, and the
	// in-memory table resets.
	c.Add(14 * time.Hour) // 2026-09-19 00:00
	tab.onTick(c.Now())

	oldPlain := filepath.Join(dir, "udpshunt-clients-2026-09-18.jsonl")
	oldGz := oldPlain + ".gz"
	waitFor(t, 2*time.Second, func() bool {
		_, err := os.Stat(oldGz)
		return err == nil
	})
	waitForAbsent(t, 2*time.Second, oldPlain)

	if tab.Tracked() != 0 {
		t.Fatalf("tracked=%d after rollover, want 0", tab.Tracked())
	}

	// Check the gzipped final snapshot content.
	f, err := os.Open(oldGz)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var lines []wireRow
	sc := bufio.NewScanner(gz)
	for sc.Scan() {
		var wr wireRow
		if err := json.Unmarshal(sc.Bytes(), &wr); err != nil {
			t.Fatalf("bad snapshot line %q: %v", sc.Text(), err)
		}
		lines = append(lines, wr)
	}
	f.Close()
	if len(lines) != 1 || lines[0].IP != "10.0.0.1" || lines[0].Requests != 2 || lines[0].BytesIn != 15 {
		t.Fatalf("snapshot content wrong: %+v", lines)
	}

	// New day counts from zero; its own snapshot persists and recovers.
	tab.PacketIn(ip(t, "10.0.0.2"), 7)
	c.Add(31 * time.Second) // past the 30s snapshot interval
	tab.onTick(c.Now())
	day2 := filepath.Join(dir, "udpshunt-clients-2026-09-19.jsonl")
	waitFor(t, 2*time.Second, func() bool {
		_, err := os.Stat(day2)
		return err == nil
	})
	tab.Stop()

	// A fresh table on day 2 continues the day's totals.
	c2 := &fakeClock{now: c.Now()}
	tab2 := New(Options{Dir: dir, SnapshotInterval: 30 * time.Second, TickEvery: time.Hour, Clock: c2.Now})
	defer tab2.Stop()
	rows, tracked, _ := tab2.Top("requests", false, 0)
	if tracked != 1 || len(rows) != 1 || rows[0].IP != "10.0.0.2" || rows[0].Requests != 1 || rows[0].BytesIn != 7 {
		t.Fatalf("day-2 recovery wrong: tracked=%d rows=%+v", tracked, rows)
	}
	// The old day is not merged into the new day.
	for _, r := range rows {
		if r.IP == "10.0.0.1" {
			t.Fatalf("day-1 IP leaked into day 2: %+v", rows)
		}
	}
}

func TestSnapshotRestartRecovery(t *testing.T) {
	c := newFakeClock(t)
	var dir string
	tab := newTestTable(t, c, func(o *Options) { dir = o.Dir })

	tab.PacketIn(ip(t, "203.0.113.7"), 74)
	tab.PacketOut(ip(t, "203.0.113.7"), 120)
	c.Add(31 * time.Second) // past the snapshot interval
	tab.onTick(c.Now())

	snap := filepath.Join(dir, "udpshunt-clients-2026-09-18.jsonl")
	data, err := os.ReadFile(snap)
	if err != nil {
		t.Fatalf("snapshot not written: %v", err)
	}
	if !strings.Contains(string(data), `"ip":"203.0.113.7"`) ||
		!strings.Contains(string(data), `"requests":1`) ||
		!strings.Contains(string(data), `"bytes_out":120`) {
		t.Fatalf("snapshot content unexpected: %s", data)
	}
	tab.Stop()

	// Restart mid-day: totals continue, including last_seen (kept for
	// faithful eviction order).
	tab2 := New(Options{Dir: dir, SnapshotInterval: 30 * time.Second, TickEvery: time.Hour, Clock: c.Now})
	defer tab2.Stop()
	rows, tracked, _ := tab2.Top("requests", false, 0)
	if tracked != 1 || len(rows) != 1 {
		t.Fatalf("tracked=%d rows=%d, want 1/1", tracked, len(rows))
	}
	if rows[0].Requests != 1 || rows[0].Responses != 1 || rows[0].BytesIn != 74 || rows[0].BytesOut != 120 {
		t.Fatalf("recovered row wrong: %+v", rows[0])
	}

	// New activity accumulates on top of the recovered baseline.
	tab2.PacketIn(ip(t, "203.0.113.7"), 10)
	rows, _, _ = tab2.Top("requests", false, 0)
	if rows[0].Requests != 2 {
		t.Fatalf("requests after restart = %d, want 2", rows[0].Requests)
	}
}

func TestDisabledWhenDirUncreatable(t *testing.T) {
	base := t.TempDir()
	blocker := filepath.Join(base, "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := newFakeClock(t)
	tab := New(Options{Dir: filepath.Join(blocker, "sub"), TickEvery: time.Hour, Clock: c.Now})
	defer tab.Stop()
	if tab.Enabled() {
		t.Fatal("table should be disabled when the dir cannot be created")
	}
	tab.PacketIn(ip(t, "10.0.0.1"), 1) // must not panic or record
	if _, _, evicted := tab.Top("requests", false, 0); evicted != 0 || tab.Tracked() != 0 {
		t.Fatal("disabled table must not record")
	}
}

func TestTempFilesRemovedOnStart(t *testing.T) {
	c := newFakeClock(t)
	dir := t.TempDir()
	stray := filepath.Join(dir, "udpshunt-clients-123456.tmp")
	if err := os.WriteFile(stray, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	tab := New(Options{Dir: dir, SnapshotInterval: 30 * time.Second, TickEvery: time.Hour, Clock: c.Now})
	defer tab.Stop()
	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatalf("stray temp file still present: %v", err)
	}
}

func TestConcurrentRecord(t *testing.T) {
	c := newFakeClock(t)
	tab := newTestTable(t, c, nil)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				tab.PacketIn(ip(t, "10.0.1."+string(rune('0'+g))), 10)
				tab.PacketOut(ip(t, "10.0.1."+string(rune('0'+g))), 20)
			}
		}(g)
	}
	wg.Wait()
	rows, tracked, _ := tab.Top("requests", false, 0)
	if tracked != 8 || len(rows) != 8 {
		t.Fatalf("tracked=%d rows=%d, want 8/8", tracked, len(rows))
	}
	for _, r := range rows {
		if r.Requests != 500 || r.Responses != 500 || r.BytesIn != 5000 || r.BytesOut != 10000 {
			t.Fatalf("counters lost under concurrency: %+v", r)
		}
	}
}

func TestNilTableIsNoop(t *testing.T) {
	var tab *Table
	tab.PacketIn(ip(t, "10.0.0.1"), 1)
	tab.PacketOut(ip(t, "10.0.0.1"), 1)
	if rows, _, _ := tab.Top("requests", false, 0); rows != nil {
		t.Fatal("nil table Top must return nil")
	}
	if tab.Enabled() || tab.Tracked() != 0 || tab.Evicted() != 0 {
		t.Fatal("nil table accessors must be zero")
	}
	tab.Stop() // must not panic
}
