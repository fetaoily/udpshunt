package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDirectoryListingSuppressed(t *testing.T) {
	h := Handler()
	ts := httptest.NewServer(h)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/assets/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /assets/ = %d, want 404", resp.StatusCode)
	}
	body := make([]byte, 1024)
	n, _ := resp.Body.Read(body)
	if strings.Contains(strings.ToLower(string(body[:n])), "<a href") {
		t.Fatal("directory listing leaked")
	}
}

func TestIndexServedAtRoot(t *testing.T) {
	h := Handler()
	ts := httptest.NewServer(h)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / = %d", resp.StatusCode)
	}
}
