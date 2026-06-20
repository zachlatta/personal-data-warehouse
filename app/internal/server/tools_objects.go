package server

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const getObjectDescription = "Fetch a stored blob object (Gmail attachment, Apple Notes/Messages attachment, Voice Memo audio, ...) by its storage reference, or a Slack attachment by its Slack file id. Pass the storage_file_id from a warehouse row's storage_* columns, or a slack_files.file_id (F...) to fetch the file live from the Slack API. Returns object metadata and a signed, time-limited download_url that fetches the raw file bytes without further authentication."

type getObjectInput struct {
	StorageFileID  string `json:"storage_file_id" jsonschema:"the storage_file_id from a warehouse row (storage_file_id / metadata_storage_file_id / html_storage_file_id), or a Slack file id from slack_files.file_id"`
	StorageBackend string `json:"storage_backend,omitempty" jsonschema:"optional storage_backend from the same row; defaults to the configured backend"`
	StorageKey     string `json:"storage_key,omitempty" jsonschema:"optional storage_key for context; not required to fetch"`
	Account        string `json:"account,omitempty" jsonschema:"required for google_drive_source rows: the row's account, identifying which Google Drive account owns the file"`
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

// driveSourceBackend marks rows whose bytes live in the user's own Google Drive
// (Drive-as-a-source), served live via a per-account store rather than copied
// into the warehouse's transport object store.
const driveSourceBackend = "google_drive_source"

// driveConnectionFromConfig builds the Google Drive connection (HTTP client and
// REST base URLs) shared by the read store and the per-source ingest stores.
// When ObjectStoreGoogleDriveDisableAuth is set (local integration testing
// against a fake Drive) it uses a plain client instead of the OAuth one.
func driveConnectionFromConfig(cfg config.Config) (objectstore.GoogleDriveConnection, error) {
	conn := objectstore.GoogleDriveConnection{
		APIBaseURL:    cfg.ObjectStoreGoogleDriveAPIBaseURL,
		UploadBaseURL: cfg.ObjectStoreGoogleDriveUploadBaseURL,
	}
	if cfg.ObjectStoreGoogleDriveDisableAuth {
		conn.HTTPClient = http.DefaultClient
		return conn, nil
	}
	client, err := objectstore.HTTPClientFromAuthorizedUserJSON(context.Background(), cfg.ObjectStoreGoogleTokenJSON)
	if err != nil {
		return objectstore.GoogleDriveConnection{}, err
	}
	conn.HTTPClient = client
	return conn, nil
}

func driveSourceConnectionFromConfig(cfg config.Config, token string) (objectstore.GoogleDriveConnection, error) {
	conn := objectstore.GoogleDriveConnection{
		APIBaseURL:    cfg.ObjectStoreGoogleDriveAPIBaseURL,
		UploadBaseURL: cfg.ObjectStoreGoogleDriveUploadBaseURL,
	}
	if cfg.ObjectStoreGoogleDriveDisableAuth {
		conn.HTTPClient = http.DefaultClient
		return conn, nil
	}
	client, err := objectstore.HTTPClientFromAuthorizedUserJSON(context.Background(), token)
	if err != nil {
		return objectstore.GoogleDriveConnection{}, err
	}
	conn.HTTPClient = client
	return conn, nil
}

// driveSourceStoresFromConfig builds one Google Drive store per configured
// source account so the download proxy can stream a file from the account that
// owns it. No folder id is needed: files are fetched by Drive file id.
func driveSourceStoresFromConfig(cfg config.Config) (map[string]objectstore.ObjectStore, error) {
	stores := map[string]objectstore.ObjectStore{}
	for account, token := range cfg.DriveSourceTokensByAccount {
		conn, err := driveSourceConnectionFromConfig(cfg, token)
		if err != nil {
			return nil, err
		}
		store, err := objectstore.BuildObjectStore(objectstore.GoogleDriveSpec(
			"",
			driveSourceBackend,
			"google_drive_file",
			"google_drive_metadata",
			nil,
			conn,
		))
		if err != nil {
			return nil, err
		}
		stores[account] = store
	}
	return stores, nil
}

// objectStoreFromConfig builds the app's object storage layer from config, or
// returns (nil, false) when object storage is not configured.
func objectStoreFromConfig(cfg config.Config) (objectstore.ObjectStore, bool, error) {
	if !cfg.ObjectStoreEnabled() {
		return nil, false, nil
	}
	conn, err := driveConnectionFromConfig(cfg)
	if err != nil {
		return nil, false, err
	}
	store, err := objectstore.BuildObjectStore(objectstore.GoogleDriveSpec(
		cfg.ObjectStoreGoogleDriveFolderID,
		"personal_data_warehouse",
		"object_blob",
		"object_metadata",
		nil,
		conn,
	))
	if err != nil {
		return nil, false, err
	}
	return store, true, nil
}

func signedObjectDownloadURL(signer *pdwauth.Service, baseURL, fileID, account string, exp time.Time) string {
	link := strings.TrimRight(baseURL, "/") + objectsPathPrefix + url.PathEscape(fileID) +
		"?exp=" + strconv.FormatInt(exp.Unix(), 10) +
		"&sig=" + signer.SignObjectDownload(fileID, account, exp)
	if account != "" {
		link += "&account=" + url.QueryEscape(account)
	}
	return link
}

func getObjectTool(store objectstore.ObjectStore, driveStores map[string]objectstore.ObjectStore, signer *pdwauth.Service, baseURL string, ttl time.Duration, now func() time.Time) tool.Tool {
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
			out := getObjectOutput{StorageFileID: in.StorageFileID, StorageKey: in.StorageKey}
			if store != nil {
				out.Backend = store.Backend()
			}
			if in.StorageFileID == "" {
				out.Error = "storage_file_id is required"
				return out, nil
			}
			selectedStore := store
			// google_drive_source files live in the user's own Drive; route to
			// the per-account store and bind the account into the signed link.
			account := ""
			if in.StorageBackend == driveSourceBackend {
				driveStore, ok := driveStores[in.Account]
				if !ok {
					out.Error = "no Google Drive credentials configured for account " + in.Account
					return out, nil
				}
				selectedStore = driveStore
				account = in.Account
				out.Backend = driveSourceBackend
			} else if selectedStore == nil {
				out.Error = "object storage is not configured"
				return out, nil
			}
			meta, err := selectedStore.GetMetadata(ctx, ref)
			if err == objectstore.ErrNotFound {
				out.Exists = false
				return out, nil
			}
			if err != nil {
				out.Error = err.Error()
				return out, nil
			}
			out.Exists = true
			if meta.Backend != "" {
				out.Backend = meta.Backend
			}
			out.ContentType = meta.ContentType
			out.SizeBytes = meta.SizeBytes
			out.ContentSHA256 = meta.ContentSHA256
			out.Filename = meta.Filename
			out.StorageURL = meta.StorageURL
			exp := now().Add(ttl)
			out.DownloadURL = signedObjectDownloadURL(signer, baseURL, in.StorageFileID, account, exp)
			out.ExpiresAt = exp.UTC().Format(time.RFC3339)
			return out, nil
		},
		IsError: func(o getObjectOutput) bool { return o.Error != "" },
	}
}
