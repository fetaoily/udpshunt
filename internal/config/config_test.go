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

const m2Config = `
listeners:
  - name: dns-in
    bind: 127.0.0.1:19000
    backends: [127.0.0.1:19001]
    balance: least_sessions
    health_check:
      mode: raw
      payload: "0001020a"
      interval: 2s
      timeout: 500ms
      rise: 2
      fall: 3
admin:
  bind: 127.0.0.1:19155
`

func TestLoadM2Fields(t *testing.T) {
	c, err := Load(writeConfig(t, m2Config))
	if err != nil {
		t.Fatal(err)
	}
	hc := c.Listeners[0].HealthCheck
	if hc.Mode != "raw" || hc.Payload != "0001020a" {
		t.Fatalf("bad health check: %+v", hc)
	}
	if time.Duration(hc.Interval) != 2*time.Second || time.Duration(hc.Timeout) != 500*time.Millisecond {
		t.Fatalf("bad durations: %+v", hc)
	}
	if hc.Rise != 2 || hc.Fall != 3 {
		t.Fatalf("bad rise/fall: %+v", hc)
	}
	if c.Admin.Bind != "127.0.0.1:19155" {
		t.Fatalf("bad admin bind: %q", c.Admin.Bind)
	}
}

func TestHealthCheckDefaults(t *testing.T) {
	c, err := Load(writeConfig(t, `
listeners:
  - name: a
    bind: 127.0.0.1:19000
    backends: [127.0.0.1:19001]
    balance: source_hash
    health_check:
      mode: dns
`))
	if err != nil {
		t.Fatal(err)
	}
	hc := c.Listeners[0].HealthCheck
	if time.Duration(hc.Interval) != 5*time.Second || time.Duration(hc.Timeout) != 1*time.Second {
		t.Fatalf("bad defaults: %+v", hc)
	}
	if hc.Rise != 2 || hc.Fall != 3 {
		t.Fatalf("bad rise/fall defaults: %+v", hc)
	}
}

func TestM2ValidationErrors(t *testing.T) {
	base := `
listeners:
  - name: a
    bind: 127.0.0.1:19000
    backends: [127.0.0.1:19001]
`
	cases := map[string]string{
		"bad balance":             base + "    balance: magic\n",
		"bad hc mode":             base + "    health_check:\n      mode: tcp\n",
		"raw no payload":          base + "    health_check:\n      mode: raw\n",
		"raw bad payload":         base + "    health_check:\n      mode: raw\n      payload: zz\n",
		"hc timeout >= interval":  base + "    health_check:\n      mode: dns\n      interval: 1s\n      timeout: 2s\n",
		"bad rise":                base + "    health_check:\n      mode: dns\n      rise: -1\n",
		"bad admin bind":          base + "admin:\n  bind: nope\n",
		"negative global timeout": base + "sessions:\n  timeout: -1s\n",
		"unknown field":           base + "sessions:\n  mx: 1\n",
	}
	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeConfig(t, yaml)); err == nil {
				t.Fatalf("expected error for %s", name)
			}
		})
	}
}
