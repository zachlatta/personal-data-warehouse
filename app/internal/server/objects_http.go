package server

import (
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
)

const objectsPathPrefix = "/objects/"

// objectDownloadHandler serves raw object bytes at GET /objects/{storage_file_id}
// guarded by the exp/sig query parameters produced by get_object. The link is
// the only credential, so signature failures get one generic message and the
// sig value itself never reaches logs.
func objectDownloadHandler(store objectstore.ObjectStore, signer *pdwauth.Service, maxBytes int64, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		fileID := strings.TrimPrefix(r.URL.Path, objectsPathPrefix)
		if fileID == "" || strings.Contains(fileID, "/") {
			http.NotFound(w, r)
			return
		}
		query := r.URL.Query()
		if err := signer.VerifyObjectDownload(fileID, query.Get("exp"), query.Get("sig")); err != nil {
			logger.WarnContext(r.Context(), "object download link rejected", "path", r.URL.Path, "error", err)
			http.Error(w, "invalid or expired link", http.StatusForbidden)
			return
		}
		ref := objectstore.StoredObject{StorageFileID: fileID}
		meta, err := store.GetMetadata(r.Context(), ref)
		if err == objectstore.ErrNotFound {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			logger.ErrorContext(r.Context(), "object metadata fetch failed", "path", r.URL.Path, "error", err)
			http.Error(w, "object store error", http.StatusBadGateway)
			return
		}
		if maxBytes > 0 && meta.SizeBytes > maxBytes {
			http.Error(w, "object too large to serve", http.StatusRequestEntityTooLarge)
			return
		}
		contentType := meta.ContentType
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if meta.Filename != "" {
			w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": meta.Filename}))
		}
		if meta.SizeBytes > 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(meta.SizeBytes, 10))
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		data, err := store.GetObject(r.Context(), ref)
		if err != nil {
			logger.ErrorContext(r.Context(), "object download failed", "path", r.URL.Path, "error", err)
			http.Error(w, "object store error", http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		_, _ = w.Write(data)
	})
}
