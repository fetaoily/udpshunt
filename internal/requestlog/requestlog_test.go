package requestlog

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fixedClock returns a clock stuck at start and a func to advance it.
func fixedClock(start time.Time) (func() time.Time, func(time.Duration)) {
	now := start
	return func() time.Time { return now }, func(d time.Duration) { now = now.Add(d) }
}

func waitFor(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never appeared", path)
}

// waitForAbsent waits until path is gone; the plain file is removed only
// after its gzip is fully written, so absence proves completeness.
func waitForAbsent(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never disappeared", path)
}

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad JSON line %q: %v", sc.Text(), err)
		}
		out = append(out, m)
	}
	return out
}

func entryAt(now time.Time) Entry {
	return Entry{
		Time:     now,
		Listener: "dns",
		Client:   "203.0.113.7:51820",
		Backend:  "10.0.0.2:53",
		Bytes:    74,
		Outcome:  OutcomeForwarded,
	}
}

func TestRecordWritesJSONLines(t *testing.T) {
	now := time.Date(2026, 9, 17, 14, 30, 5, 0, time.Local)
	clock, _ := fixedClock(now)
	dir := t.TempDir()
	l := New(Options{Dir: dir, RetentionDays: 30, Clock: clock, FlushEvery: time.Hour})
	l.Record(entryAt(now))
	l.Record(Entry{Time: now, Listener: "dns", Client: "198.51.100.9:5000", Bytes: 20, Outcome: OutcomeNoBackend})
	l.Stop()

	lines := readLines(t, filepath.Join(dir, "udpshunt-requests-2026-09-17.log"))
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d", len(lines))
	}
	first := lines[0]
	if first["listener"] != "dns" || first["client"] != "203.0.113.7:51820" ||
		first["backend"] != "10.0.0.2:53" || first["outcome"] != "forwarded" {
		t.Fatalf("bad first line: %v", first)
	}
	if bv, ok := first["bytes"].(float64); !ok || bv != 74 {
		t.Fatalf("bad bytes: %v", first["bytes"])
	}
	if !strings.Contains(first["time"].(string), "14:30:05") {
		t.Fatalf("bad time: %v", first["time"])
	}
	// backend omitted when empty (omitempty)
	if _, ok := lines[1]["backend"]; ok {
		t.Fatalf("empty backend must be omitted: %v", lines[1])
	}
}

func TestRotationCompressesPreviousDay(t *testing.T) {
	now := time.Date(2026, 9, 17, 23, 59, 58, 0, time.Local)
	clock, advance := fixedClock(now)
	dir := t.TempDir()
	l := New(Options{Dir: dir, RetentionDays: 30, Clock: clock, FlushEvery: time.Hour})
	l.Record(entryAt(now))
	advance(5 * time.Second) // past midnight
	l.Record(entryAt(clock()))
	l.Stop()

	lines := readLines(t, filepath.Join(dir, "udpshunt-requests-2026-09-18.log"))
	if len(lines) != 1 {
		t.Fatalf("new day file should hold only the post-rollover line, got %d", len(lines))
	}
	// The previous day's file is replaced by its gzip (async: poll for the
	// plain file's removal, which happens after the gzip is complete).
	plain := filepath.Join(dir, "udpshunt-requests-2026-09-17.log")
	gzPath := plain + ".gz"
	waitForAbsent(t, plain)
	if _, err := os.Stat(gzPath); err != nil {
		t.Fatalf("compressed previous-day file missing: %v", err)
	}
	f, err := os.Open(gzPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("not valid gzip: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, gz); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "203.0.113.7:51820") {
		t.Fatalf("gzipped content lost the original entry: %q", buf.String())
	}
}

func TestRestartMidDayAppends(t *testing.T) {
	now := time.Date(2026, 9, 17, 8, 0, 0, 0, time.Local)
	clock, _ := fixedClock(now)
	dir := t.TempDir()
	l1 := New(Options{Dir: dir, RetentionDays: 30, Clock: clock, FlushEvery: time.Hour})
	l1.Record(entryAt(now))
	l1.Stop()
	l2 := New(Options{Dir: dir, RetentionDays: 30, Clock: clock, FlushEvery: time.Hour})
	l2.Record(entryAt(now))
	l2.Stop()

	lines := readLines(t, filepath.Join(dir, "udpshunt-requests-2026-09-17.log"))
	if len(lines) != 2 {
		t.Fatalf("restart must append, not truncate: got %d lines", len(lines))
	}
}

func TestPruneRemovesExpiredFiles(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.Local)
	clock, _ := fixedClock(now)
	dir := t.TempDir()
	files := map[string]bool{
		"udpshunt-requests-2026-08-01.log":    false, // 47 days old: removed
		"udpshunt-requests-2026-08-01.log.gz": false, // removed
		"udpshunt-requests-2026-08-18.log":    true,  // exactly cutoff: kept
		"udpshunt-requests-2026-09-16.log":    true,  // recent: kept
		"udpshunt-requests-2026-09-16.log.gz": true,
		"unrelated.txt":                       true,
	}
	for name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	l := New(Options{Dir: dir, RetentionDays: 30, Clock: clock, FlushEvery: time.Hour})
	l.Stop()
	for name, wantKept := range files {
		_, err := os.Stat(filepath.Join(dir, name))
		exists := err == nil
		if exists != wantKept {
			t.Fatalf("%s: exists = %v, want kept = %v", name, exists, wantKept)
		}
	}
}

func TestSweepStaleCompressesCrashLeftovers(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.Local)
	clock, _ := fixedClock(now)
	dir := t.TempDir()
	// A crash mid-rotation left yesterday's plain file behind.
	stale := filepath.Join(dir, "udpshunt-requests-2026-09-16.log")
	if err := os.WriteFile(stale, []byte("{\"x\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	l := New(Options{Dir: dir, RetentionDays: 30, Clock: clock, FlushEvery: time.Hour})
	l.Stop()
	waitForAbsent(t, stale)
	if _, err := os.Stat(stale + ".gz"); err != nil {
		t.Fatalf("compressed leftover missing: %v", err)
	}
}

func TestRecordDropsWhenQueueFull(t *testing.T) {
	clock, _ := fixedClock(time.Date(2026, 9, 17, 12, 0, 0, 0, time.Local))
	l := New(Options{Dir: t.TempDir(), RetentionDays: 30, QueueSize: 1, Clock: clock, FlushEvery: time.Hour})
	l.Stop() // writer exits; the queue stops draining
	l.Record(entryAt(clock()))
	l.Record(entryAt(clock())) // queue full: dropped
	if got := l.Dropped(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	if !l.Enabled() {
		t.Fatal("logger should still count as enabled after Stop")
	}
}

func TestDisabledWhenDirUncreatable(t *testing.T) {
	dir := t.TempDir()
	notADir := filepath.Join(dir, "file")
	if err := os.WriteFile(notADir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	l := New(Options{Dir: filepath.Join(notADir, "sub"), Clock: time.Now})
	if l.Enabled() {
		t.Fatal("logger must be disabled when the directory cannot be created")
	}
	l.Record(entryAt(time.Now())) // must not panic
	l.Stop()                      // must not hang
	if got := l.Dropped(); got != 0 {
		t.Fatalf("disabled logger must not count drops, got %d", got)
	}
}

func TestConcurrentRecord(t *testing.T) {
	clock, _ := fixedClock(time.Date(2026, 9, 17, 12, 0, 0, 0, time.Local))
	dir := t.TempDir()
	l := New(Options{Dir: dir, RetentionDays: 30, QueueSize: 8192, Clock: clock, FlushEvery: time.Hour})
	const goroutines, per = 8, 100
	done := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < per; i++ {
				l.Record(entryAt(clock()))
			}
		}()
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}
	l.Stop()
	lines := readLines(t, filepath.Join(dir, "udpshunt-requests-2026-09-17.log"))
	if len(lines) != goroutines*per {
		t.Fatalf("want %d lines, got %d (dropped %d)", goroutines*per, len(lines), l.Dropped())
	}
}
