package payloadfilter

import (
	"encoding/hex"
	"strings"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("test hex %q: %v", s, err)
	}
	return b
}

func mustCompile(t *testing.T, cfg []RuleConfig) *Rules {
	t.Helper()
	r, err := Compile(cfg)
	if err != nil {
		t.Fatalf("Compile(%+v): %v", cfg, err)
	}
	if r == nil {
		t.Fatal("Compile returned a nil gate for a non-empty config")
	}
	return r
}

func TestCompile(t *testing.T) {
	t.Run("empty or nil input compiles to the nil gate", func(t *testing.T) {
		for _, cfg := range [][]RuleConfig{nil, {}} {
			r, err := Compile(cfg)
			if err != nil {
				t.Fatalf("Compile(%v) = %v, want no error", cfg, err)
			}
			if r != nil {
				t.Fatal("empty input must compile to a nil *Rules (gate off)")
			}
		}
	})

	t.Run("single rule with defaults filled", func(t *testing.T) {
		r := mustCompile(t, []RuleConfig{{MagicHex: "5763536a"}})
		if r.Len() != 1 {
			t.Fatalf("Len = %d, want 1", r.Len())
		}
		magic := mustHex(t, "5763536a")
		if !r.Allowed(magic) {
			t.Fatal("a packet of exactly offset+len(magic) must pass the default min_length")
		}
		if r.Allowed(magic[:3]) {
			t.Fatal("a packet one byte short must fail the default min_length")
		}
	})

	t.Run("multiple rules", func(t *testing.T) {
		r := mustCompile(t, []RuleConfig{
			{MagicHex: "aabbcc"},
			{MagicHex: "5763536a", Offset: 2, MinLength: 10},
		})
		if r.Len() != 2 {
			t.Fatalf("Len = %d, want 2", r.Len())
		}
	})

	t.Run("errors name the offending rule index", func(t *testing.T) {
		cases := []struct {
			name string
			cfg  []RuleConfig
			want string
		}{
			{"bad hex", []RuleConfig{{MagicHex: "zz"}}, `rules[0]: magic_hex must be hex`},
			{"empty magic_hex", []RuleConfig{{MagicHex: ""}}, `rules[0]: magic_hex is required`},
			{"negative offset", []RuleConfig{{MagicHex: "5763536a", Offset: -1}}, `rules[0]: offset must be >= 0`},
			{"offset+magic beyond the packet bound", []RuleConfig{{MagicHex: "5763536a", Offset: 65534}}, `rules[0]: offset+magic (65538 bytes) exceeds the 65536-byte packet bound`},
			{"negative min_length", []RuleConfig{{MagicHex: "5763536a", MinLength: -1}}, `rules[0]: min_length must be >= 0`},
			{"index points at the second rule", []RuleConfig{{MagicHex: "5763536a"}, {MagicHex: "zz"}}, `rules[1]: magic_hex must be hex`},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				r, err := Compile(tc.cfg)
				if err == nil {
					t.Fatalf("Compile(%+v) must fail", tc.cfg)
				}
				if r != nil {
					t.Fatal("a failed Compile must return a nil *Rules")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error %q must mention %q", err, tc.want)
				}
			})
		}
	})
}

func TestAllowed(t *testing.T) {
	magic := mustHex(t, "5763536a")

	t.Run("default min_length is offset+len(magic)", func(t *testing.T) {
		r := mustCompile(t, []RuleConfig{{MagicHex: "5763536a"}}) // offset 0, min_length 4
		frame := append(append([]byte{}, magic...), 0xDE, 0xAD, 0xBE, 0xEF)
		flipped := append([]byte{}, frame...)
		flipped[1] ^= 0xFF
		cases := []struct {
			name string
			pkt  []byte
			want bool
		}{
			{"frame starting with the magic", frame, true},
			{"one flipped magic byte", flipped, false},
			{"3-byte packet is shorter than min_length", magic[:3], false},
			{"empty packet", []byte{}, false},
		}
		for _, tc := range cases {
			if got := r.Allowed(tc.pkt); got != tc.want {
				t.Errorf("%s: Allowed(% x) = %v, want %v", tc.name, tc.pkt, got, tc.want)
			}
		}
	})

	t.Run("explicit min_length", func(t *testing.T) {
		r := mustCompile(t, []RuleConfig{{MagicHex: "5763536a", MinLength: 8}})
		if r.Allowed(magic) {
			t.Fatal("a 4-byte magic-only packet must not pass an 8-byte min_length")
		}
		full := append(append([]byte{}, magic...), 0, 0, 0, 0)
		if !r.Allowed(full) {
			t.Fatal("an 8-byte packet carrying the magic must pass")
		}
	})

	t.Run("min_length smaller than the rule footprint", func(t *testing.T) {
		r := mustCompile(t, []RuleConfig{{MagicHex: "aabbccdd", Offset: 4, MinLength: 2}})
		atOffset := append([]byte{0, 0, 0, 0}, mustHex(t, "aabbccdd")...) // magic at bytes [4:8]
		cases := []struct {
			name string
			pkt  []byte
			want bool
		}{
			// Below offset+len(magic): the second length check must reject
			// the packet before the pkt[4:8] slice would panic.
			{"short packet below the rule footprint", []byte{0x01, 0x02, 0x03, 0x04, 0x05}, false},
			{"packet carrying the magic at offset 4", atOffset, true},
		}
		for _, tc := range cases {
			if got := r.Allowed(tc.pkt); got != tc.want {
				t.Errorf("%s: Allowed(% x) = %v, want %v", tc.name, tc.pkt, got, tc.want)
			}
		}
	})

	t.Run("nonzero offset", func(t *testing.T) {
		r := mustCompile(t, []RuleConfig{{MagicHex: "5763536a", Offset: 2}})
		at := append([]byte{0x00, 0x00}, magic...) // magic at bytes [2:6]
		if !r.Allowed(at) {
			t.Fatal("magic at bytes [2:6] must pass an offset-2 rule")
		}
		before := append(append([]byte{}, magic...), 0x00, 0x00) // magic at bytes [0:4]
		if r.Allowed(before) {
			t.Fatal("magic at bytes [0:4] must not pass an offset-2 rule")
		}
	})

	t.Run("any-of across rules", func(t *testing.T) {
		r := mustCompile(t, []RuleConfig{
			{MagicHex: "aabbcc"},
			{MagicHex: "5763536a"},
		})
		pkt := append(append([]byte{}, magic...), 0xFF) // matches only the second rule
		if !r.Allowed(pkt) {
			t.Fatal("a packet matching only the second rule must pass (any-of)")
		}
		if r.Allowed([]byte("xyzw")) {
			t.Fatal("a packet matching no rule must fail")
		}
	})

	t.Run("nil gate allows everything", func(t *testing.T) {
		var r *Rules
		if !r.Allowed(nil) {
			t.Fatal("the nil gate must allow a nil packet")
		}
		if !r.Allowed([]byte("any payload")) {
			t.Fatal("the nil gate must allow any packet")
		}
		if r.Len() != 0 {
			t.Fatalf("nil gate Len = %d, want 0", r.Len())
		}
	})
}
