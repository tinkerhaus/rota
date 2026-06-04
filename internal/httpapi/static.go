package httpapi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// spaFallback serves files from staticDir. For a client-side route (a path with
// no file extension that doesn't resolve to a real file) it falls back to
// index.html so deep links into the SPA work. If staticDir or index.html is
// absent the request simply 404s — the Go build never depends on a frontend
// build existing (no go:embed yet; that consolidation is deferred).
func spaFallback(staticDir string, fs http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// API paths never fall through here (they are matched first), but guard
		// anyway so a stray /api/* under the catch-all can't serve index.html.
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		clean := filepath.Clean(r.URL.Path)
		full := filepath.Join(staticDir, clean)
		if info, err := os.Stat(full); err == nil && !info.IsDir() {
			fs.ServeHTTP(w, r)
			return
		}
		// Unknown path with no extension ⇒ SPA client route ⇒ serve index.html.
		index := filepath.Join(staticDir, "index.html")
		if filepath.Ext(clean) == "" {
			if _, err := os.Stat(index); err == nil {
				http.ServeFile(w, r, index)
				return
			}
		}
		http.NotFound(w, r)
	})
}

// decodeJSON reads a small JSON request body into v, writing a 400 on failure.
// Returns false if it already wrote an error response.
func decodeJSON(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	if err := dec.Decode(v); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}
