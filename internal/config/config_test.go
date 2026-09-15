package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "udpshunt.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const fullConfig = `
listeners:
  - name: dns-in
    bind: 0.0.0.0:53
    backends: [10.0.0.1:53, 10.0.0.2:53]
    balance: round_robin
    session_timeout: 30s
sessions:
  timeout: 60s
  max: 1000
logging:
  level: debug
  format: text
`

func TestLoadFullConfig(t *testing.T) {
	c, err := Load(writeConfig(t, fullConfig))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Listeners) != 1 {
		t.Fatalf("want 1 listener, got %d", len(c.Listeners))
	}
	l := c.Listeners[0]
	if l.Name != "dns-in" || l.Bind != "0.0.0.0:53" {
		t.Fatalf("bad listener: %+v", l)
	}
	if len(l.Backends) != 2 || l.Backends[0] != "10.0.0.1:53" {
		t.Fatalf("bad backends: %v", l.Backends)
	}
	if l.Balance != "round_robin" {
		t.Fatalf("bad balance: %q", l.Balance)
	}
	if time.Duration(l.SessionTimeout) != 30*time.Second {
		t.Fatalf("bad session_timeout: %v", l.SessionTimeout)
	}
	if time.Duration(c.Sessions.Timeout) != 60*time.Second || c.Sessions.Max != 1000 {
		t.Fatalf("bad sessions: %+v", c.Sessions)
	}
	if c.Logging.Level != "debug" || c.Logging.Format != "text" {
		t.Fatalf("bad logging: %+v", c.Logging)
	}
}

func TestDefaults(t *testing.T) {
	c, err := Load(writeConfig(t, `
listeners:
  - name: a
    bind: 127.0.0.1:9000
    backends: [127.0.0.1:9001]
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listeners[0].Balance != "round_robin" {
		t.Fatalf("default balance = %q", c.Listeners[0].Balance)
	}
	if time.Duration(c.Listeners[0].SessionTimeout) != 60*time.Second {
		t.Fatalf("listener timeout should default to global, got %v", c.Listeners[0].SessionTimeout)
	}
	if c.Logging.Level != "info" || c.Logging.Format != "json" {
		t.Fatalf("default logging: %+v", c.Logging)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]string{
		"no listeners":   "sessions:\n  timeout: 60s\n",
		"missing name":   "listeners:\n  - bind: 127.0.0.1:1\n    backends: [127.0.0.1:2]\n",
		"bad bind":       "listeners:\n  - name: a\n    bind: not-an-addr\n    backends: [127.0.0.1:2]\n",
		"no backends":    "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: []\n",
		"bad backend":    "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: [oops]\n",
		"bad balance":    "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: [127.0.0.1:2]\n    balance: magic\n",
		"bad timeout":    "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: [127.0.0.1:2]\n    session_timeout: -1s\n",
		"bad level":      "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: [127.0.0.1:2]\nlogging:\n  level: loud\n",
		"bad format":     "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: [127.0.0.1:2]\nlogging:\n  format: xml\n",
		"bad yaml":       "listeners: [unclosed\n",
		"bad duration":   "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: [127.0.0.1:2]\n    session_timeout: soon\n",
		"duplicate name": "listeners:\n  - name: a\n    bind: 127.0.0.1:1\n    backends: [127.0.0.1:2]\n  - name: a\n    bind: 127.0.0.1:3\n    backends: [127.0.0.1:4]\n",
	}
	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, yaml)); err == nil {
				t.Fatalf("expected error for %s", name)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}
