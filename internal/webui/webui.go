// Package webui embeds the built single-page UI and serves it under /ui.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// Handler serves the embedded UI. The returned handler expects requests
// with the /ui prefix already stripped by the caller.
func Handler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic(err) // embed layout is static; unreachable
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			// Serve index.html at the bare /ui/ path.
			r.URL.Path = "/"
		}
		if _, err := fs.Stat(sub, p); err != nil {
			// SPA fallback: unknown paths render the app shell.
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})
}
