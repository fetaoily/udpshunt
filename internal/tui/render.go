// Package tui renders a live udpshunt dashboard (spec §9.2). It is a pure
// client of the admin HTTP API.
package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/fetaoily/udpshunt/internal/admin"
)

var blocks = []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

var (
	upStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	downStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	titleStyle = lipgloss.NewStyle().Bold(true)
	dimStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
)

// sparkline renders values as a width-capped block-char bar. Nil/empty
// input renders as "".
func sparkline(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return ""
	}
	if len(values) > width {
		values = values[len(values)-width:]
	}
	lo, hi := values[0], values[0]
	for _, v := range values {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	span := hi - lo
	var b strings.Builder
	for _, v := range values {
		idx := 0
		if span > 0 {
			idx = int((v - lo) / span * float64(len(blocks)-1))
		}
		b.WriteRune(blocks[idx])
	}
	return b.String()
}

// healthText renders a colored UP/DOWN marker.
func healthText(healthy bool) string {
	if healthy {
		return upStyle.Render("UP")
	}
	return downStyle.Render("DOWN")
}

// totalView is the summed per-second rate across all listeners.
type totalView struct {
	InPPS, OutPPS float64
	InBPS, OutBPS float64
}

// totals derives per-second rates for the whole daemon from the last two
// cumulative history samples (prev -> cur).
func totals(prev, cur map[string][]admin.Sample, interval time.Duration) totalView {
	var tv totalView
	if interval <= 0 {
		return tv
	}
	for name, samples := range cur {
		if len(samples) == 0 {
			continue
		}
		last := samples[len(samples)-1]
		p, ok := firstAtOrBefore(prev[name], last.T)
		if !ok {
			continue
		}
		dt := last.T - p.T
		if dt <= 0 {
			continue
		}
		sec := float64(dt) / 1000
		tv.InPPS += float64(last.In-p.In) / sec
		tv.OutPPS += float64(last.Out-p.Out) / sec
		tv.InBPS += float64(last.BytesIn-p.BytesIn) / sec
		tv.OutBPS += float64(last.BytesOut-p.BytesOut) / sec
	}
	return tv
}

func firstAtOrBefore(samples []admin.Sample, t int64) (admin.Sample, bool) {
	var best admin.Sample
	found := false
	for _, s := range samples {
		if s.T <= t {
			best = s
			found = true
		}
	}
	return best, found
}

// humanBytes formats a byte count (e.g. 12.3 MiB).
func humanBytes(v float64) string {
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%.0f B", v)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	i := -1
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i+1])
}

// listenerRows is the per-listener view data the model renders.
type listenerRow struct {
	Name     string
	Bind     string
	Balance  string
	Sessions int64
	Healthy  int
	Backends int
	InLine   string
	OutLine  string
}

func listenerRows(history map[string][]admin.Sample, sts []admin.ListenerStatus, width int) []listenerRow {
	rows := make([]listenerRow, 0, len(sts))
	for _, ls := range sts {
		samples := history[ls.Name]
		rows = append(rows, listenerRow{
			Name:     ls.Name,
			Bind:     ls.Bind,
			Balance:  ls.Balance,
			Sessions: ls.Sessions,
			Healthy:  countHealthy(ls.Backends),
			Backends: len(ls.Backends),
			InLine:   sparkline(rateSeries(samples, true), width),
			OutLine:  sparkline(rateSeries(samples, false), width),
		})
	}
	return rows
}

func countHealthy(bs []admin.BackendStatus) int {
	n := 0
	for _, b := range bs {
		if b.Healthy {
			n++
		}
	}
	return n
}

// rateSeries converts cumulative history into per-second deltas.
func rateSeries(samples []admin.Sample, in bool) []float64 {
	vals := make([]float64, 0, len(samples))
	for i := 1; i < len(samples); i++ {
		dt := samples[i].T - samples[i-1].T
		if dt <= 0 {
			continue
		}
		sec := float64(dt) / 1000
		if in {
			vals = append(vals, float64(samples[i].In-samples[i-1].In)/sec)
		} else {
			vals = append(vals, float64(samples[i].Out-samples[i-1].Out)/sec)
		}
	}
	return vals
}
