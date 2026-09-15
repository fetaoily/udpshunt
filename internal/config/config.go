// Package config loads and validates udpshunt configuration.
package config

import (
	"fmt"
	"net"
	"os"
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
}

type Listener struct {
	Name           string   `yaml:"name"`
	Bind           string   `yaml:"bind"`
	Backends       []string `yaml:"backends"`
	Balance        string   `yaml:"balance"`
	SessionTimeout Duration `yaml:"session_timeout"`
}

type Sessions struct {
	Timeout Duration `yaml:"timeout"`
	Max     int      `yaml:"max"`
}

type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Load reads the YAML file at path, applies defaults, and validates the result.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
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
		for _, b := range l.Backends {
			if _, err := net.ResolveUDPAddr("udp", b); err != nil {
				return fmt.Errorf("listeners[%d] (%s): invalid backend %q: %w", i, l.Name, b, err)
			}
		}
		if l.Balance != "round_robin" {
			return fmt.Errorf("listeners[%d] (%s): unsupported balance %q (M1 supports round_robin)", i, l.Name, l.Balance)
		}
		if l.SessionTimeout <= 0 {
			return fmt.Errorf("listeners[%d] (%s): session_timeout must be positive", i, l.Name)
		}
	}
	if c.Sessions.Max < 0 {
		return fmt.Errorf("sessions.max must be >= 0")
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
