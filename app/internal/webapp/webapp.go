// Package webapp serves the browser UI: a single-page app over the same
// bearer-protected JSON API the iOS app uses (/api/timeline*, /api/mutations/*,
// /api/tools/search). Nothing here renders data server-side; the Go binary
// only ships the static shell, so the two clients cannot drift apart on what
// the API returns.
package webapp

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"net/http"
	"path"
	"sort"
	"strings"
)

//go:embed static
var staticFS embed.FS

// AssetPrefix is where the SPA's JS/CSS are served from. Assets are
// referenced with a content-hash query string so they can be cached hard
// while the shell itself is never cached.
const AssetPrefix = "/app/"

// Routes are the browser paths the SPA owns; every one of them, and every
// path beneath it, returns the same shell and the JS router takes over.
var Routes = []string{"/timeline", "/mutation-review", "/search"}

// ReviewPath is the review UI's root, kept stable because approval URLs
// returned to agents and pushed to the phone point under it.
const ReviewPath = "/mutation-review"

type handler struct {
	assets  http.Handler
	shell   []byte
	version string
}

// New builds the SPA handler. It reads the embedded shell once, stamps the
// asset version into it, and serves everything from memory.
func New() (http.Handler, error) {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, err
	}
	version, err := assetVersion(sub)
	if err != nil {
		return nil, err
	}
	shell, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		return nil, err
	}
	shell = []byte(strings.ReplaceAll(string(shell), "__ASSET_VERSION__", version))
	return &handler{
		assets:  http.StripPrefix(AssetPrefix, http.FileServer(http.FS(sub))),
		shell:   shell,
		version: version,
	}, nil
}

// Register mounts the shell on every SPA route and the assets on AssetPrefix.
func Register(mux *http.ServeMux, h http.Handler) {
	mux.Handle(AssetPrefix, h)
	for _, route := range Routes {
		mux.Handle(route, h)
		mux.Handle(route+"/", h)
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, AssetPrefix) {
		if r.URL.Path == AssetPrefix || path.Base(r.URL.Path) == "index.html" {
			http.NotFound(w, r)
			return
		}
		// Assets are addressed by content hash, so a stale cache is impossible
		// and a long max-age is safe; an unversioned request must revalidate.
		if r.URL.Query().Get("v") == h.version {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		h.assets.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The shell is tiny and iterated on; never let a browser cache a stale copy.
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(h.shell)
}

// assetVersion hashes every embedded asset so the version changes whenever
// any of them does.
func assetVersion(files fs.FS) (string, error) {
	var names []string
	err := fs.WalkDir(files, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && name != "index.html" {
			names = append(names, name)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	sum := sha256.New()
	for _, name := range names {
		data, err := fs.ReadFile(files, name)
		if err != nil {
			return "", err
		}
		sum.Write([]byte(name))
		sum.Write(data)
	}
	return hex.EncodeToString(sum.Sum(nil))[:12], nil
}
