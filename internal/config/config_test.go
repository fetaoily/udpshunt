package config

import (
	"os"
	"path/filepath"
	"strings"
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
	validateCases := map[string]string{
		"bad balance":             base + "    balance: magic\n",
		"bad hc mode":             base + "    health_check:\n      mode: tcp\n",
		"raw no payload":          base + "    health_check:\n      mode: raw\n",
		"raw bad payload":         base + "    health_check:\n      mode: raw\n      payload: zz\n",
		"hc timeout >= interval":  base + "    health_check:\n      mode: dns\n      interval: 1s\n      timeout: 2s\n",
		"bad rise":                base + "    health_check:\n      mode: dns\n      rise: -1\n",
		"bad admin bind":          base + "admin:\n  bind: nope\n",
		"negative global timeout": base + "sessions:\n  timeout: -1s\n",
	}
	for name, yaml := range validateCases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, yaml))
			if err == nil {
				t.Fatalf("expected error for %s", name)
			}
			if !strings.HasPrefix(err.Error(), "invalid config: ") {
				t.Fatalf("error for %s did not come from Validate: %v", name, err)
			}
		})
	}
	parseCases := map[string]string{
		"unknown field": base + "sessions:\n  mx: 1\n",
	}
	for name, yaml := range parseCases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, yaml))
			if err == nil {
				t.Fatalf("expected error for %s", name)
			}
			if !strings.HasPrefix(err.Error(), "parse config: ") {
				t.Fatalf("error for %s did not come from YAML parsing: %v", name, err)
			}
		})
	}
}

func TestReadBufferConfig(t *testing.T) {
	c, err := Load(writeConfig(t, `
listeners:
  - name: a
    bind: 127.0.0.1:19000
    backends: [127.0.0.1:19001]
    read_buffer: 8388608
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listeners[0].ReadBuffer != 8388608 {
		t.Fatalf("read_buffer = %d", c.Listeners[0].ReadBuffer)
	}
	_, err = Load(writeConfig(t, `
listeners:
  - name: a
    bind: 127.0.0.1:19000
    backends: [127.0.0.1:19001]
    read_buffer: -1
`))
	if err == nil {
		t.Fatal("negative read_buffer must be rejected")
	}
}

func TestM3ValidationErrors(t *testing.T) {
	listener := func(name, backends string) string {
		return "listeners:\n  - name: " + name + "\n    bind: 127.0.0.1:19000\n    backends: [" + backends + "]\n"
	}
	cases := map[string]string{
		"duplicate backends":         listener("a", "127.0.0.1:19001, 127.0.0.1:19001"),
		"aliased duplicate backends": listener("a", `"127.0.0.1:19001", "localhost:19001"`),
		"name with pipe":             listener(`"a|b"`, "127.0.0.1:19001"),
	}
	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, yaml))
			if err == nil {
				t.Fatalf("expected error for %s", name)
			}
			if !strings.HasPrefix(err.Error(), "invalid config: ") {
				t.Fatalf("error for %s did not come from Validate: %v", name, err)
			}
		})
	}
}

const baseListener = `
listeners:
  - name: a
    bind: 127.0.0.1:19000
    backends: [127.0.0.1:19001]
`

func TestRequestLogDefaultsEnabled(t *testing.T) {
	c, err := Load(writeConfig(t, baseListener))
	if err != nil {
		t.Fatal(err)
	}
	if !c.RequestLog.IsEnabled() {
		t.Fatal("request log must default to enabled")
	}
	if c.RequestLog.RetentionDays != 30 {
		t.Fatalf("default retention_days = %d, want 30", c.RequestLog.RetentionDays)
	}
	if c.RequestLog.Dir == "" {
		t.Fatal("default dir must not be empty")
	}
}

func TestRequestLogExplicitDisable(t *testing.T) {
	c, err := Load(writeConfig(t, baseListener+"request_log:\n  enabled: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.RequestLog.IsEnabled() {
		t.Fatal("enabled: false must disable request logging")
	}
}

func TestRequestLogExplicitSettings(t *testing.T) {
	c, err := Load(writeConfig(t, baseListener+`
request_log:
  enabled: true
  dir: /tmp/rl
  retention_days: 7
`))
	if err != nil {
		t.Fatal(err)
	}
	if !c.RequestLog.IsEnabled() || c.RequestLog.Dir != "/tmp/rl" || c.RequestLog.RetentionDays != 7 {
		t.Fatalf("bad request_log: %+v", c.RequestLog)
	}
}

func TestRequestLogValidationErrors(t *testing.T) {
	cases := map[string]string{
		// retention_days: 0 cannot be distinguished from "unset" with a
		// plain int, so it normalizes to the default (30); only negatives
		// are rejected.
		"negative retention": baseListener + "request_log:\n  retention_days: -3\n",
	}
	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, yaml))
			if err == nil {
				t.Fatalf("expected error for %s", name)
			}
			if !strings.HasPrefix(err.Error(), "invalid config: ") {
				t.Fatalf("error for %s did not come from Validate: %v", name, err)
			}
		})
	}
}

func TestClientStatsDefaultsEnabled(t *testing.T) {
	c, err := Load(writeConfig(t, baseListener))
	if err != nil {
		t.Fatal(err)
	}
	if !c.ClientStats.IsEnabled() {
		t.Fatal("client stats must default to enabled")
	}
	if c.ClientStats.RetentionDays != 30 {
		t.Fatalf("default retention_days = %d, want 30", c.ClientStats.RetentionDays)
	}
	if c.ClientStats.MaxIPs != 65536 {
		t.Fatalf("default max_ips = %d, want 65536", c.ClientStats.MaxIPs)
	}
	if c.ClientStats.SnapshotInterval != Duration(60*time.Second) {
		t.Fatalf("default snapshot_interval = %v, want 60s", c.ClientStats.SnapshotInterval)
	}
	if c.ClientStats.Dir == "" {
		t.Fatal("default dir must not be empty")
	}
}

func TestClientStatsExplicitDisable(t *testing.T) {
	c, err := Load(writeConfig(t, baseListener+"client_stats:\n  enabled: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.ClientStats.IsEnabled() {
		t.Fatal("enabled: false must disable client stats")
	}
}

func TestClientStatsExplicitSettings(t *testing.T) {
	c, err := Load(writeConfig(t, baseListener+`
client_stats:
  enabled: true
  dir: /tmp/cs
  retention_days: 7
  max_ips: 1024
  snapshot_interval: 15s
`))
	if err != nil {
		t.Fatal(err)
	}
	if !c.ClientStats.IsEnabled() || c.ClientStats.Dir != "/tmp/cs" ||
		c.ClientStats.RetentionDays != 7 || c.ClientStats.MaxIPs != 1024 ||
		c.ClientStats.SnapshotInterval != Duration(15*time.Second) {
		t.Fatalf("bad client_stats: %+v", c.ClientStats)
	}
}

func TestClientStatsValidationErrors(t *testing.T) {
	cases := map[string]string{
		// retention_days: 0 / max_ips: 0 normalize to defaults (plain ints
		// cannot distinguish unset from zero); only negatives are rejected.
		"negative retention": baseListener + "client_stats:\n  retention_days: -3\n",
		"negative max_ips":   baseListener + "client_stats:\n  max_ips: -1\n",
		"negative interval":  baseListener + "client_stats:\n  snapshot_interval: -5s\n",
	}
	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, yaml))
			if err == nil {
				t.Fatalf("expected error for %s", name)
			}
			if !strings.HasPrefix(err.Error(), "invalid config: ") {
				t.Fatalf("error for %s did not come from Validate: %v", name, err)
			}
		})
	}
}

func TestOnDownValidation(t *testing.T) {
	cases := map[string]struct {
		yaml    string
		wantErr bool
		wantPol string
	}{
		"absent defaults to close": {yaml: baseListener, wantErr: false, wantPol: ""},
		"close accepted":           {yaml: baseListener + "    on_down: close\n", wantErr: false, wantPol: "close"},
		"drain accepted":           {yaml: baseListener + "    on_down: drain\n", wantErr: false, wantPol: "drain"},
		"invalid rejected":         {yaml: baseListener + "    on_down: nuke\n", wantErr: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := Load(writeConfig(t, tc.yaml))
			if tc.wantErr {
				if err == nil {
					t.Fatal("invalid on_down must be rejected")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := c.Listeners[0].OnDown; got != tc.wantPol {
				t.Fatalf("OnDown = %q, want %q", got, tc.wantPol)
			}
		})
	}
}

const pfListener = `
listeners:
  - name: t
    bind: 127.0.0.1:0
    backends: [127.0.0.1:65001]
`

func TestPayloadFilterSection(t *testing.T) {
	c, err := Load(writeConfig(t, pfListener+`
    payload_filter:
      rules:
        - magic_hex: "00112233"
          offset: 0
          min_length: 8
      log_illegal: true
`))
	if err != nil {
		t.Fatal(err)
	}
	pf := c.Listeners[0].PayloadFilter
	if len(pf.Rules) != 1 {
		t.Fatalf("want 1 rule, got %d", len(pf.Rules))
	}
	r := pf.Rules[0]
	if r.MagicHex != "00112233" || r.Offset != 0 || r.MinLength != 8 {
		t.Fatalf("bad rule: %+v", r)
	}
	if !pf.LogIllegal {
		t.Fatal("log_illegal must parse")
	}
}

func TestPayloadFilterMinLengthOmitted(t *testing.T) {
	// min_length omitted stays 0 in the config; Compile fills it in (covered
	// by the payloadfilter package tests), config parsing must not.
	c, err := Load(writeConfig(t, pfListener+`
    payload_filter:
      rules:
        - magic_hex: "00112233"
          offset: 4
`))
	if err != nil {
		t.Fatal(err)
	}
	r := c.Listeners[0].PayloadFilter.Rules[0]
	if r.MagicHex != "00112233" || r.Offset != 4 || r.MinLength != 0 {
		t.Fatalf("rule must keep min_length 0, got %+v", r)
	}
}

func TestPayloadFilterAbsent(t *testing.T) {
	c, err := Load(writeConfig(t, pfListener))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Listeners[0].PayloadFilter.Rules) != 0 || c.Listeners[0].PayloadFilter.LogIllegal {
		t.Fatalf("absent section must be the zero value, got %+v", c.Listeners[0].PayloadFilter)
	}
}

func TestPayloadFilterValidationErrors(t *testing.T) {
	cases := map[string]string{
		"bad hex":         "    payload_filter:\n      rules:\n        - magic_hex: \"zz\"\n",
		"negative offset": "    payload_filter:\n      rules:\n        - magic_hex: \"00112233\"\n          offset: -1\n",
		"negative length": "    payload_filter:\n      rules:\n        - magic_hex: \"00112233\"\n          min_length: -1\n",
	}
	for name, yaml := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, pfListener+yaml))
			if err == nil {
				t.Fatalf("expected error for %s", name)
			}
			if !strings.HasPrefix(err.Error(), "invalid config: ") {
				t.Fatalf("error for %s did not come from Validate: %v", name, err)
			}
			if !strings.Contains(err.Error(), "listeners[0] (t)") {
				t.Fatalf("error for %s missing listener prefix: %v", name, err)
			}
			if !strings.Contains(err.Error(), "payload_filter") {
				t.Fatalf("error for %s missing payload_filter: %v", name, err)
			}
		})
	}
}

func TestBlacklistValidation(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "bl.txt")
	os.WriteFile(good, []byte("203.0.113.7,198.51.100.0/24"), 0o600)
	bad := filepath.Join(dir, "bad.txt")
	os.WriteFile(bad, []byte("not-an-ip"), 0o600)

	cases := map[string]struct {
		yaml    string
		wantErr bool
	}{
		"absent":              {baseListener + "blacklist:\n  entries: [203.0.113.7]\n", false},
		"file and entries":    {baseListener + "blacklist:\n  entries: [192.0.2.1]\n  file: " + good + "\n", false},
		"invalid entry":       {baseListener + "blacklist:\n  entries: [nope]\n", true},
		"prefix on host bits": {baseListener + "blacklist:\n  entries: [10.0.0.1/24]\n", true},
		"missing file":        {baseListener + "blacklist:\n  file: " + filepath.Join(dir, "nope.txt") + "\n", true},
		"invalid in file":     {baseListener + "blacklist:\n  file: " + bad + "\n", true},
		"negative watch":      {baseListener + "blacklist:\n  watch_interval: -1s\n", true},
		"watch zero ok":       {baseListener + "blacklist:\n  watch_interval: 0s\n", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.yaml))
			if tc.wantErr != (err != nil) {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
