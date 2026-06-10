package server

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const getObjectDescription = "Fetch a stored blob object (Gmail attachment, Apple Notes/Messages attachment, Voice Memo audio, ...) by its storage reference. Pass the storage_file_id from a warehouse row's storage_* columns. Returns object metadata and a signed, time-limited download_url that fetches the raw file bytes without further authentication."

type getObjectInput struct {
	StorageFileID  string `json:"storage_file_id" jsonschema:"the storage_file_id from a warehouse row (storage_file_id / metadata_storage_file_id / html_storage_file_id)"`
	StorageBackend string `json:"storage_backend,omitempty" jsonschema:"optional storage_backend from the same row; defaults to the configured backend"`
	StorageKey     string `json:"storage_key,omitempty" jsonschema:"optional storage_key for context; not required to fetch"`
}

type getObjectOutput struct {
	StorageFileID string `json:"storage_file_id"`
	StorageKey    string `json:"storage_key,omitempty"`
	Backend       string `json:"storage_backend"`
	Exists        bool   `json:"exists"`
	ContentType   string `json:"content_type,omitempty"`
	SizeBytes     int64  `json:"size_bytes,omitempty"`
	ContentSHA256 string `json:"content_sha256,omitempty"`
	Filename      string `json:"filename,omitempty"`
	StorageURL    string `json:"storage_url,omitempty"`
	DownloadURL   string `json:"download_url,omitempty"`
	ExpiresAt     string `json:"expires_at,omitempty"`
	Error         string `json:"error,omitempty"`
}

// objectStoreFromConfig builds the app's object storage layer from config, or
// returns (nil, false) when object storage is not configured.
func objectStoreFromConfig(cfg config.Config) (objectstore.ObjectStore, bool, error) {
	if !cfg.ObjectStoreEnabled() {
		return nil, false, nil
	}
	client, err := objectstore.HTTPClientFromAuthorizedUserJSON(context.Background(), cfg.ObjectStoreGoogleTokenJSON)
	if err != nil {
		return nil, false, err
	}
	store, err := objectstore.BuildObjectStore(objectstore.GoogleDriveSpec(
		cfg.ObjectStoreGoogleDriveFolderID,
		"personal_data_warehouse",
		"object_blob",
		"object_metadata",
		nil,
		objectstore.GoogleDriveConnection{HTTPClient: client},
	))
	if err != nil {
		return nil, false, err
	}
	return store, true, nil
}

func signedObjectDownloadURL(signer *pdwauth.Service, baseURL, fileID string, exp time.Time) string {
	return strings.TrimRight(baseURL, "/") + objectsPathPrefix + url.PathEscape(fileID) +
		"?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&sig=" + signer.SignObjectDownload(fileID, exp)
}

func getObjectTool(store objectstore.ObjectStore, signer *pdwauth.Service, baseURL string, ttl time.Duration, now func() time.Time) tool.Tool {
	return &tool.Typed[getObjectInput, getObjectOutput]{
		NameStr:        "get_object",
		TitleStr:       "Get Stored Object",
		DescriptionStr: getObjectDescription,
		Handle: func(ctx context.Context, in getObjectInput) (getObjectOutput, error) {
			ref := objectstore.StoredObject{
				StorageBackend: in.StorageBackend,
				StorageKey:     in.StorageKey,
				StorageFileID:  in.StorageFileID,
			}
			out := getObjectOutput{StorageFileID: in.StorageFileID, StorageKey: in.StorageKey, Backend: store.Backend()}
			if in.StorageFileID == "" {
				out.Error = "storage_file_id is required"
				return out, nil
			}
			meta, err := store.GetMetadata(ctx, ref)
			if err == objectstore.ErrNotFound {
				out.Exists = false
				return out, nil
			}
			if err != nil {
				out.Error = err.Error()
				return out, nil
			}
			out.Exists = true
			out.ContentType = meta.ContentType
			out.SizeBytes = meta.SizeBytes
			out.ContentSHA256 = meta.ContentSHA256
			out.Filename = meta.Filename
			out.StorageURL = meta.StorageURL
			exp := now().Add(ttl)
			out.DownloadURL = signedObjectDownloadURL(signer, baseURL, in.StorageFileID, exp)
			out.ExpiresAt = exp.UTC().Format(time.RFC3339)
			return out, nil
		},
		IsError: func(o getObjectOutput) bool { return o.Error != "" },
	}
}
