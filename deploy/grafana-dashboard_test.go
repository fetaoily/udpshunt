package deploy

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestDashboardJSONIsValid(t *testing.T) {
	raw, err := os.ReadFile("grafana-dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		Title         string `json:"title"`
		SchemaVersion int    `json:"schemaVersion"`
		Panels        []struct {
			Type    string `json:"type"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}
	if dash.Title != "udpshunt" || len(dash.Panels) < 8 {
		t.Fatalf("title=%q panels=%d", dash.Title, len(dash.Panels))
	}
	// Every metric family the README promises must appear somewhere.
	rawText := string(raw)
	for _, metric := range []string{
		"udpshunt_packets_in_total", "udpshunt_packets_out_total",
		"udpshunt_bytes_in_total", "udpshunt_bytes_out_total",
		"udpshunt_sessions_active", "udpshunt_sessions_rejected_total",
		"udpshunt_backend_healthy", "udpshunt_backend_errors_total",
	} {
		if !strings.Contains(rawText, metric) {
			t.Errorf("dashboard does not reference %s", metric)
		}
	}
	// The datasource variable must be defined, or ${DS_PROMETHEUS} breaks import.
	if !strings.Contains(rawText, "\"DS_PROMETHEUS\"") {
		t.Error("datasource template variable DS_PROMETHEUS missing")
	}
}
