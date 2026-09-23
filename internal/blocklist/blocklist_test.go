package blocklist

import (
	"net/netip"
	"os"
	"path/filepath"
	"testing"
)

func mustNew(t *testing.T, entries ...string) *List {
	t.Helper()
	l, err := New(entries)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestParseNormalizeAndBlock(t *testing.T) {
	l := mustNew(t, "203.0.113.7", "198.51.100.0/24", "2001:db8::/32",
		"203.0.113.7") // duplicate silently dropped
	if l.Len() != 3 {
		t.Fatalf("Len = %d, want 3 (dedupe)", l.Len())
	}
	cases := map[string]bool{
		"203.0.113.7":  true,  // exact
		"198.51.100.9": true,  // inside CIDR
		"198.51.101.9": false, // outside CIDR
		"2001:db8::1":  true,  // v6 prefix
		"2001:db9::1":  false,
		"192.0.2.1":    false,
	}
	for ip, want := range cases {
		a, err := netip.ParseAddr(ip)
		if err != nil {
			t.Fatal(err)
		}
		if got := l.Blocked(a.Unmap()); got != want {
			t.Fatalf("Blocked(%s) = %v, want %v", ip, got, want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, bad := range []string{"not-an-ip", "10.0.0.0/99", "10.0.0.1/24", "", "10.0.0.0/8/7"} {
		if _, err := New([]string{bad}); err == nil {
			t.Fatalf("New(%q) must fail", bad)
		}
	}
}

func TestEntriesCanonical(t *testing.T) {
	l := mustNew(t, "203.0.113.7")
	if got := l.Entries(); len(got) != 1 || got[0] != "203.0.113.7/32" {
		t.Fatalf("Entries = %v, want [203.0.113.7/32]", got)
	}
}

func TestContainerSwapAndNilSafety(t *testing.T) {
	c := NewContainer()
	a, _ := netip.ParseAddr("203.0.113.7")
	if c.Blocked(a.Unmap()) {
		t.Fatal("empty container must not block")
	}
	c.Swap(mustNew(t, "203.0.113.7"))
	if !c.Blocked(a.Unmap()) {
		t.Fatal("must block after swap")
	}
}

func TestReadFileEntries(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bl.txt")
	if err := os.WriteFile(p, []byte("203.0.113.7, 198.51.100.0/24\n\n192.0.2.9\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFileEntries(p)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"203.0.113.7", "198.51.100.0/24", "192.0.2.9"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	if _, err := ReadFileEntries(filepath.Join(t.TempDir(), "missing.txt")); err == nil {
		t.Fatal("missing file must error (fail-closed)")
	}
}
