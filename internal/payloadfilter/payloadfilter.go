// Package payloadfilter implements the listener ingress payload gate: an
// immutable, compiled set of magic-byte rules any of which a packet may
// match to be considered legal.
package payloadfilter

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// maxPacket mirrors pktio.MaxPacketSize without importing it: the gate runs
// on receive-loop buffers whose payloads never exceed this size.
const maxPacket = 65536

// RuleConfig is one raw allowlist rule as spelled in YAML.
type RuleConfig struct {
	MagicHex  string `yaml:"magic_hex"`
	Offset    int    `yaml:"offset"`
	MinLength int    `yaml:"min_length"` // 0 -> offset+len(magic) at compile time
}

type compiledRule struct {
	magic  []byte
	offset int
	minLen int
}

// Rules is an immutable compiled snapshot. The nil *Rules is valid and
// allows every packet (gate off).
type Rules struct {
	rules []compiledRule
}

// Compile validates and compiles rules. An empty or nil input compiles to
// a nil *Rules (gate off). Errors name the offending rule index.
func Compile(cfg []RuleConfig) (*Rules, error) {
	if len(cfg) == 0 {
		return nil, nil
	}
	rules := make([]compiledRule, 0, len(cfg))
	for i, c := range cfg {
		magic, err := hex.DecodeString(c.MagicHex)
		if err != nil {
			return nil, fmt.Errorf("rules[%d]: magic_hex must be hex: %w", i, err)
		}
		if len(magic) == 0 {
			return nil, fmt.Errorf("rules[%d]: magic_hex is required", i)
		}
		if c.Offset < 0 {
			return nil, fmt.Errorf("rules[%d]: offset must be >= 0", i)
		}
		if c.Offset+len(magic) > maxPacket {
			return nil, fmt.Errorf("rules[%d]: offset+magic (%d bytes) exceeds the %d-byte packet bound", i, c.Offset+len(magic), maxPacket)
		}
		if c.MinLength < 0 {
			return nil, fmt.Errorf("rules[%d]: min_length must be >= 0", i)
		}
		minLen := c.MinLength
		if minLen == 0 {
			minLen = c.Offset + len(magic)
		}
		rules = append(rules, compiledRule{magic: magic, offset: c.Offset, minLen: minLen})
	}
	return &Rules{rules: rules}, nil
}

// Allowed reports whether pkt carries at least one rule's magic bytes at
// the rule's offset. A nil receiver allows every packet (gate off). The
// second length check keeps the slice bounds safe when a configured
// min_length is smaller than the rule's own footprint.
func (r *Rules) Allowed(pkt []byte) bool {
	if r == nil {
		return true
	}
	for i := range r.rules {
		c := &r.rules[i]
		if len(pkt) >= c.minLen && len(pkt) >= c.offset+len(c.magic) &&
			bytes.Equal(pkt[c.offset:c.offset+len(c.magic)], c.magic) {
			return true
		}
	}
	return false
}

// Len returns the number of compiled rules (0 for the nil gate).
func (r *Rules) Len() int {
	if r == nil {
		return 0
	}
	return len(r.rules)
}
