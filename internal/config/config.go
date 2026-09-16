// Package config loads and validates udpshunt configuration.
package config

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration so YAML accepts values like "60s".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

type Config struct {
	Listeners []Listener `yaml:"listeners"`
	Sessions  Sessions   `yaml:"sessions"`
	Logging   Logging    `yaml:"logging"`
	Admin     Admin      `yaml:"admin"`
}

type Listener struct {
	Name           string      `yaml:"name"`
	Bind           string      `yaml:"bind"`
	Backends       []string    `yaml:"backends"`
	Balance        string      `yaml:"balance"`
	SessionTimeout Duration    `yaml:"session_timeout"`
	HealthCheck    HealthCheck `yaml:"health_check"`
}

type Sessions struct {
	Timeout Duration `yaml:"timeout"`
	Max     int      `yaml:"max"`
}

type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type HealthCheck struct {
	Mode     string   `yaml:"mode"` // none | raw | dns
	Interval Duration `yaml:"interval"`
	Timeout  Duration `yaml:"timeout"`
	Rise     int      `yaml:"rise"`
	Fall     int      `yaml:"fall"`
	Payload  string   `yaml:"payload"` // hex bytes, raw mode only
}

type Admin struct {
	Bind string `yaml:"bind"`
}

// Load reads the YAML file at path, applies defaults, and validates the result.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&c)
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return c, nil
}

func applyDefaults(c *Config) {
	if c.Sessions.Timeout == 0 {
		c.Sessions.Timeout = Duration(60 * time.Second)
	}
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "json"
	}
	for i := range c.Listeners {
		if c.Listeners[i].Balance == "" {
			c.Listeners[i].Balance = "round_robin"
		}
		if c.Listeners[i].SessionTimeout == 0 {
			c.Listeners[i].SessionTimeout = c.Sessions.Timeout
		}
		hc := &c.Listeners[i].HealthCheck
		if hc.Interval == 0 {
			hc.Interval = Duration(5 * time.Second)
		}
		if hc.Timeout == 0 {
			hc.Timeout = Duration(1 * time.Second)
		}
		if hc.Rise == 0 {
			hc.Rise = 2
		}
		if hc.Fall == 0 {
			hc.Fall = 3
		}
	}
	if c.Admin.Bind == "" {
		c.Admin.Bind = "127.0.0.1:9155"
	}
}

func (c Config) Validate() error {
	if len(c.Listeners) == 0 {
		return fmt.Errorf("at least one listener is required")
	}
	seen := make(map[string]bool, len(c.Listeners))
	for i, l := range c.Listeners {
		if l.Name == "" {
			return fmt.Errorf("listeners[%d]: name is required", i)
		}
		if strings.Contains(l.Name, "|") {
			// "|" is the session-key and backend-count map separator; a name
			// containing it could collide with another listener's keys.
			return fmt.Errorf("listeners[%d] (%s): name must not contain %q", i, l.Name, "|")
		}
		if seen[l.Name] {
			return fmt.Errorf("listeners[%d] (%s): duplicate name", i, l.Name)
		}
		seen[l.Name] = true
		if _, err := net.ResolveUDPAddr("udp", l.Bind); err != nil {
			return fmt.Errorf("listeners[%d] (%s): invalid bind %q: %w", i, l.Name, l.Bind, err)
		}
		if len(l.Backends) == 0 {
			return fmt.Errorf("listeners[%d] (%s): at least one backend is required", i, l.Name)
		}
		seenBackends := make(map[string]bool, len(l.Backends))
		for _, b := range l.Backends {
			if _, err := net.ResolveUDPAddr("udp", b); err != nil {
				return fmt.Errorf("listeners[%d] (%s): invalid backend %q: %w", i, l.Name, b, err)
			}
			if seenBackends[b] {
				return fmt.Errorf("listeners[%d] (%s): duplicate backend %q", i, l.Name, b)
			}
			seenBackends[b] = true
		}
		switch l.Balance {
		case "round_robin", "least_sessions", "source_hash":
		default:
			return fmt.Errorf("listeners[%d] (%s): unsupported balance %q", i, l.Name, l.Balance)
		}
		switch hc := l.HealthCheck; hc.Mode {
		case "", "none":
			// no checks
		case "raw":
			if _, err := hex.DecodeString(hc.Payload); err != nil {
				return fmt.Errorf("listeners[%d] (%s): health_check payload must be hex: %w", i, l.Name, err)
			}
			if hc.Payload == "" {
				return fmt.Errorf("listeners[%d] (%s): health_check mode raw requires payload", i, l.Name)
			}
		case "dns":
			// built-in query payload
		default:
			return fmt.Errorf("listeners[%d] (%s): unsupported health_check mode %q", i, l.Name, hc.Mode)
		}
		if mode := l.HealthCheck.Mode; mode != "" && mode != "none" {
			hc := l.HealthCheck
			if hc.Interval <= 0 || hc.Timeout <= 0 || hc.Rise < 1 || hc.Fall < 1 {
				return fmt.Errorf("listeners[%d] (%s): health_check interval/timeout must be positive and rise/fall >= 1", i, l.Name)
			}
			if hc.Timeout >= hc.Interval {
				return fmt.Errorf("listeners[%d] (%s): health_check timeout must be shorter than interval", i, l.Name)
			}
		}
		if l.SessionTimeout <= 0 {
			return fmt.Errorf("listeners[%d] (%s): session_timeout must be positive", i, l.Name)
		}
	}
	if c.Sessions.Max < 0 {
		return fmt.Errorf("sessions.max must be >= 0")
	}
	if c.Sessions.Timeout < 0 {
		return fmt.Errorf("sessions.timeout must be >= 0")
	}
	if _, err := net.ResolveUDPAddr("udp", c.Admin.Bind); err != nil {
		return fmt.Errorf("admin.bind: invalid address %q: %w", c.Admin.Bind, err)
	}
	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("logging.level must be one of debug|info|warn|error, got %q", c.Logging.Level)
	}
	switch c.Logging.Format {
	case "json", "text":
	default:
		return fmt.Errorf("logging.format must be json or text, got %q", c.Logging.Format)
	}
	return nil
}
