package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/fetaoily/udpshunt/internal/clientstats"
)

// wideRow exercises every failure mode of a fixed-width table: a 39-char
// IPv6 address, 7-digit counters, and byte totals at the "1023.9 MiB"
// scale that used to glue onto the neighbouring column.
func wideRow() clientstats.Row {
	return clientstats.Row{
		IP:        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
		Requests:  1234567,
		PPSIn:     1234,
		Responses: 987654,
		BPSIn:     1048576,
		BPSOut:    1048576,
		BytesIn:   1073741824 * 5,
		BytesOut:  1073741824 * 5,
	}
}

func TestClientWidthsGrowWithContent(t *testing.T) {
	now := time.Unix(0, 0)
	rows := []clientstats.Row{wideRow()}
	w := clientWidths(rows, 1, false, now)
	if w[0] < 40 {
		t.Fatalf("ip width = %d, want >= 40 (39-char IPv6 + gap)", w[0])
	}
	if w[1] < len("▼req")+1 {
		t.Fatalf("req width = %d, want >= arrow+label+gap", w[1])
	}
	// Short content keeps the floor widths.
	short := clientWidths(nil, 0, false, now)
	for i, want := range []int{15, 7, 9, 7, 9, 9, 9, 9, 7} {
		if short[i] < want {
			t.Fatalf("col %d width = %d, floor %d", i, short[i], want)
		}
	}
}

func TestClientRowKeepsColumnGap(t *testing.T) {
	now := time.Unix(0, 0)
	rows := []clientstats.Row{wideRow()}
	line := clientRow(wideRow(), clientWidths(rows, 0, false, now), now)
	// The two byte-total cells (the only "TiB" occurrences — the rate
	// columns render as "GiB/s") must keep whitespace between them: the
	// fixed-width table rendered "4.2 GiB653.8 MiB" when both columns
	// were exact fits.
	first := strings.Index(line, "TiB")
	last := strings.LastIndex(line, "TiB")
	if first == -1 || last == first {
		t.Fatalf("expected two byte-total cells: %q", line)
	}
	if line[first+3] != ' ' {
		t.Fatalf("columns glued: %q", line)
	}
}
