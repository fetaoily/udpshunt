// Package blocklist provides an immutable blacklist of client IPs (exact
// addresses and CIDR prefixes) behind a copy-on-write atomic container.
//
// A List is built once by New (parse, normalize, dedupe) and never mutated
// afterwards. The read path is lock-free: one atomic load from the
// Container, then a map probe over exact addresses and a linear scan over
// the prefix slice. Host-length prefixes (/32, /128) live in the exact
// map; shorter ones are linearly scanned — lists are sized for hundreds of
// entries, not a trie. Writes replace the whole immutable snapshot via
// Container.Swap, the same COW idiom as the balancer pool (no hot-path
// locks).
package blocklist

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"slices"
	"strings"
	"sync/atomic"
)

// ErrInvalidEntry is wrapped by entry-validation failures so HTTP layers
// can distinguish a bad entry (400) from other failures such as a list
// file that cannot be written (500).
var ErrInvalidEntry = errors.New("blocklist: invalid entry")

// List is an immutable set of blocked client IPs, normalized to netip.Prefix
// form. Exact addresses are stored as host-length prefixes in a map keyed by
// the unmapped address; shorter prefixes are kept in a slice for linear
// scanning.
type List struct {
	exact    map[netip.Addr]struct{}
	prefixes []netip.Prefix
}

// New parses entries — bare IPs and CIDR prefixes — into an immutable List.
// Bare IPs become host-length prefixes (/32 or /128) and IPv4-mapped IPv6
// addresses are unmapped to plain IPv4 form. Entries with host bits set
// under the prefix mask (e.g. "10.0.0.1/24") and unparseable entries are
// rejected; duplicates collapse silently. An empty entries slice yields an
// empty, valid List.
func New(entries []string) (*List, error) {
	l := &List{exact: make(map[netip.Addr]struct{})}
	for _, e := range entries {
		p, err := parseEntry(e)
		if err != nil {
			return nil, err
		}
		if p.Bits() == p.Addr().BitLen() {
			l.exact[p.Addr()] = struct{}{}
			continue
		}
		if !slices.Contains(l.prefixes, p) {
			l.prefixes = append(l.prefixes, p)
		}
	}
	return l, nil
}

// parseEntry normalizes one entry to an unmapped netip.Prefix. It accepts
// CIDR notation (netip.ParsePrefix) or a bare address (host-length prefix).
// A CIDR address in IPv4-mapped form is unmapped; a mapped prefix shorter
// than 96 bits describes genuine IPv6 space and is kept as-is.
func parseEntry(e string) (netip.Prefix, error) {
	if p, err := netip.ParsePrefix(e); err == nil {
		if a := p.Addr(); a.Is4In6() && p.Bits() >= 96 {
			p = netip.PrefixFrom(a.Unmap(), p.Bits()-96)
		}
		// ParsePrefix does not reject host bits set under the mask; the
		// blacklist wants one canonical form per range, so "10.0.0.1/24"
		// is a config error, not "10.0.0.0/24" in disguise.
		if p != p.Masked() {
			return netip.Prefix{}, fmt.Errorf("blocklist: entry %q has host bits set", e)
		}
		return p, nil
	}
	a, err := netip.ParseAddr(e)
	if err != nil {
		return netip.Prefix{}, fmt.Errorf("blocklist: invalid entry %q", e)
	}
	a = a.Unmap()
	return netip.PrefixFrom(a, a.BitLen()), nil
}

// Blocked reports whether a is in the list. The address is unmapped to
// canonical form before probing, so IPv4-mapped IPv6 callers match their
// IPv4 entries. The map probe is O(1); the prefix scan is linear over the
// (small) prefix slice.
func (l *List) Blocked(a netip.Addr) bool {
	a = a.Unmap()
	if _, ok := l.exact[a]; ok {
		return true
	}
	for _, p := range l.prefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// Entries returns the canonical string form of every entry (bare IPs as
// host-length prefixes), sorted for stable output.
func (l *List) Entries() []string {
	out := make([]string, 0, len(l.exact)+len(l.prefixes))
	for a := range l.exact {
		out = append(out, netip.PrefixFrom(a, a.BitLen()).String())
	}
	for _, p := range l.prefixes {
		out = append(out, p.String())
	}
	slices.Sort(out)
	return out
}

// Len returns the number of distinct entries: exact addresses plus prefixes.
func (l *List) Len() int { return len(l.exact) + len(l.prefixes) }

// Container holds the current List and swaps it atomically (copy-on-write:
// writers build a fresh List and never mutate the old one in place). The
// zero value is usable and blocks nothing until the first Swap; NewContainer
// starts with an empty List instead.
type Container struct {
	list atomic.Pointer[List]
}

// NewContainer returns a Container holding an empty (permissive) List.
func NewContainer() *Container {
	c := &Container{}
	c.list.Store(&List{})
	return c
}

// Swap atomically replaces the whole List.
func (c *Container) Swap(l *List) { c.list.Store(l) }

// Load returns the current List, or nil for a zero-value Container that was
// never swapped.
func (c *Container) Load() *List { return c.list.Load() }

// Blocked reports whether a is blocked by the current List. Nil-safe: a
// zero-value Container (no List yet) blocks nothing.
func (c *Container) Blocked(a netip.Addr) bool {
	l := c.list.Load()
	return l != nil && l.Blocked(a)
}

// ReadFileEntries reads a list file holding entries separated by commas,
// newlines, spaces or tabs; blank lines and surrounding whitespace produce
// no entries. A missing or unreadable file is an error (fail-closed: a
// mistyped path must not silently become an empty blacklist).
func ReadFileEntries(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return strings.FieldsFunc(string(b), func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == ' ' || r == '\t'
	}), nil
}
