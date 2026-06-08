package server

import (
	"context"
	"encoding/base64"

	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const getObjectDescription = "Fetch a stored blob object (Gmail attachment, Apple Notes/Messages attachment, Voice Memo audio, ...) by its storage reference. Pass the storage_file_id from a warehouse row's storage_* columns. Returns object metadata and, when within the size cap, the base64-encoded content."

type getObjectInput struct {
	StorageFileID  string `json:"storage_file_id" jsonschema:"the storage_file_id from a warehouse row (storage_file_id / metadata_storage_file_id / html_storage_file_id)"`
	StorageBackend string `json:"storage_backend,omitempty" jsonschema:"optional storage_backend from the same row; defaults to the configured backend"`
	StorageKey     string `json:"storage_key,omitempty" jsonschema:"optional storage_key for context; not required to fetch"`
}

type getObjectOutput struct {
	StorageFileID  string `json:"storage_file_id"`
	StorageKey     string `json:"storage_key,omitempty"`
	Backend        string `json:"storage_backend"`
	Exists         bool   `json:"exists"`
	ContentType    string `json:"content_type,omitempty"`
	SizeBytes      int64  `json:"size_bytes,omitempty"`
	ContentSHA256  string `json:"content_sha256,omitempty"`
	Filename       string `json:"filename,omitempty"`
	StorageURL     string `json:"storage_url,omitempty"`
	ContentBase64  string `json:"content_base64,omitempty"`
	ContentOmitted bool   `json:"content_omitted,omitempty"`
	Error          string `json:"error,omitempty"`
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

func getObjectTool(store objectstore.ObjectStore, maxBytes int64) tool.Tool {
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
			if maxBytes > 0 && meta.SizeBytes > maxBytes {
				out.ContentOmitted = true
				return out, nil
			}
			data, err := store.GetObject(ctx, ref)
			if err != nil {
				out.Error = err.Error()
				return out, nil
			}
			if maxBytes > 0 && int64(len(data)) > maxBytes {
				out.ContentOmitted = true
				return out, nil
			}
			out.ContentBase64 = base64.StdEncoding.EncodeToString(data)
			return out, nil
		},
		IsError: func(o getObjectOutput) bool { return o.Error != "" },
	}
}
