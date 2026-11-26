package server

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	_ "image/gif"
	"image/jpeg"
	_ "image/png"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/config"
	"github.com/zachlatta/personal-data-warehouse/app/internal/imaging"
	"github.com/zachlatta/personal-data-warehouse/app/internal/objectstore"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const getObjectDescription = "Fetch a stored blob object (Gmail attachment, Apple Notes/Messages attachment, screen timelapse screenshot, photo, Voice Memo audio, ...) by its storage reference, or a Slack attachment by its Slack file id. Pass the storage_file_id from a warehouse row's storage_* columns (or preview_storage_file_id on marts_screen_timelapse.active_screens / thumbnail_storage_file_id on marts_photos.photos for MCP inline viewing), or a slack_files.file_id (F...) to fetch the file live from the Slack API. Returns object metadata and a signed, time-limited download_url that fetches the raw file bytes without further authentication. Set inline=true over MCP to attach PNG/JPEG/GIF/WebP images directly as ImageContent (PNG/JPEG larger than 1568 px or 4 MiB are auto-resized to JPEG). On the CLI, pass --save <path> (converting HEIC/WebP/AVIF to .png/.jpg automatically) or fetch download_url with curl/requests, not urllib."

const (
	maxInlineImageBytes    = 4 << 20
	maxInlineRawFetchBytes = 32 << 20
	maxInlineImageEdge     = 1568
	// Cap decoded pixels before image.Decode so a small compressed PNG with
	// huge dimensions (decompression bomb) cannot allocate hundreds of MiB.
	maxInlineImagePixels = 50_000_000
)

type getObjectInput struct {
	StorageFileID  string `json:"storage_file_id" jsonschema:"the storage_file_id from a warehouse row (storage_file_id / preview_storage_file_id / thumbnail_storage_file_id / metadata_storage_file_id / html_storage_file_id), or a Slack file id from slack_files.file_id"`
	StorageBackend string `json:"storage_backend,omitempty" jsonschema:"optional storage_backend from the same row; defaults to the configured backend"`
	StorageKey     string `json:"storage_key,omitempty" jsonschema:"optional storage_key for context; not required to fetch"`
	Account        string `json:"account,omitempty" jsonschema:"required for google_drive_source rows: the row's account, identifying which Google Drive account owns the file"`
	Inline         bool   `json:"inline,omitempty" jsonschema:"MCP only: when true and the object is a viewable image (PNG, JPEG, GIF, WebP), return the image bytes inline as MCP ImageContent (auto-downscaling PNG/JPEG > 1568 px or > 4 MiB to JPEG)"`
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
	Hint          string `json:"hint,omitempty"`
	Error         string `json:"error,omitempty"`

	// inlineImage and inlineMIME carry image bytes for MCP ImageContent when
	// Inline is requested on a viewable image. Unexported so HTTP/CLI JSON
	// stays compact.
	inlineImage []byte
	inlineMIME  string
}

func normalizedMIME(contentType string) string {
	return strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
}

func isInlineDecodableImage(contentType string) bool {
	switch normalizedMIME(contentType) {
	case "image/png", "image/jpeg", "image/gif":
		return true
	default:
		return false
	}
}

func isInlineViewableImage(contentType string) bool {
	switch normalizedMIME(contentType) {
	case "image/png", "image/jpeg", "image/webp", "image/gif":
		return true
	default:
		return false
	}
}

// prepareInlineImage returns image bytes and MIME type suitable for MCP
// ImageContent (at most maxInlineImageEdge px on the long edge and at most
// maxInlineImageBytes). PNG/JPEG/GIF images are header-checked with
// image.DecodeConfig first and refused above maxInlineImagePixels so a
// compressed decompression bomb cannot exhaust memory; smaller images that
// exceed the edge or byte cap are bilinear-downscaled and JPEG-encoded in Go.
func prepareInlineImage(data []byte, contentType string) (imgBytes []byte, imgMIME string, refuseReason string, ok bool) {
	ct := normalizedMIME(contentType)
	if ct == "image/webp" {
		if int64(len(data)) <= maxInlineImageBytes {
			return data, ct, "", true
		}
		return nil, "", "size", false
	}
	if !isInlineDecodableImage(ct) {
		return nil, "", "unsupported", false
	}
	cfg, _, cfgErr := image.DecodeConfig(bytes.NewReader(data))
	if cfgErr != nil {
		// Fall back to raw bytes if already within size budget.
		if int64(len(data)) <= maxInlineImageBytes {
			return data, ct, "", true
		}
		return nil, "", "decode", false
	}
	if cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width)*int64(cfg.Height) > maxInlineImagePixels {
		return nil, "", "pixels", false
	}
	longEdge := max(cfg.Width, cfg.Height)
	if longEdge <= maxInlineImageEdge && int64(len(data)) <= maxInlineImageBytes {
		return data, ct, "", true
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		if int64(len(data)) <= maxInlineImageBytes {
			return data, ct, "", true
		}
		return nil, "", "decode", false
	}
	target := img
	if longEdge > maxInlineImageEdge {
		scale := float64(maxInlineImageEdge) / float64(longEdge)
		dstW := max(1, int(math.Round(float64(cfg.Width)*scale)))
		dstH := max(1, int(math.Round(float64(cfg.Height)*scale)))
		target = imaging.ScaleBilinear(img, dstW, dstH)
	}
	for _, q := range []int{82, 68, 55} {
		var buf bytes.Buffer
		if encErr := jpeg.Encode(&buf, target, &jpeg.Options{Quality: q}); encErr == nil && int64(buf.Len()) <= maxInlineImageBytes {
			return buf.Bytes(), "image/jpeg", "", true
		}
	}
	return nil, "", "size", false
}

// MCPCallToolResult attaches MCP ImageContent alongside the JSON metadata when
// Inline was requested and the object is a directly viewable image.
func (o getObjectOutput) MCPCallToolResult(isError bool) *mcp.CallToolResult {
	encoded, err := json.Marshal(o)
	if err != nil {
		encoded = []byte(`{"error":"failed to marshal get_object metadata"}`)
	}
	content := []mcp.Content{&mcp.TextContent{Text: string(encoded)}}
	mime := o.inlineMIME
	if mime == "" {
		mime = normalizedMIME(o.ContentType)
	}
	if !isError && len(o.inlineImage) > 0 && isInlineViewableImage(mime) {
		content = append(content, &mcp.ImageContent{
			Data:     o.inlineImage,
			MIMEType: mime,
		})
	}
	return &mcp.CallToolResult{
		Content: content,
		IsError: isError,
	}
}

// downloadURLHint travels with every download_url because the public app
// hostname sits behind Cloudflare, whose bot rule rejects Python urllib's
// default User-Agent (“Python-urllib/3.x“) with HTTP 403 before the request
// reaches the app. Measured 2026-09-09 on a freshly minted link: urllib 403,
// curl / requests / wget / Go all 200. The link itself is valid; the client
// signature is what is refused. The caller is a model that will typically
// reach for urllib, so the output says so rather than letting a valid link
// read as a broken one.
const downloadURLHint = "Fetch download_url with curl, wget, Python requests, or any client that sends a descriptive User-Agent. Python urllib's default User-Agent (Python-urllib/3.x) is rejected at the edge with HTTP 403 before the request reaches the app; the link is still valid."

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

// getObjectRequestIDHint answers a mutation request id with its status query.
func getObjectRequestIDHint(raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	for _, field := range fields {
		var value string
		if json.Unmarshal(field, &value) == nil && strings.HasPrefix(value, "req_") {
			return &tool.InvalidInputError{Message: "mutation requests are not stored objects; pdw sql \"SELECT id, status, error FROM ops.upstream_mutation_requests WHERE id = '" + strings.ReplaceAll(value, "'", "''") + "'\"; see topic mutations for operation status"}
		}
	}
	return nil
}

func getObjectTool(store objectstore.ObjectStore, driveStores map[string]objectstore.ObjectStore, signer *pdwauth.Service, baseURL string, ttl time.Duration, now func() time.Time) tool.Tool {
	return &tool.Typed[getObjectInput, getObjectOutput]{
		NameStr:        "get_object",
		TitleStr:       "Get Stored Object",
		DescriptionStr: getObjectDescription,
		ArgumentHint:   getObjectRequestIDHint,
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
			if exportMime, ok := objectstore.GoogleNativeExportMime(meta.ContentType); ok {
				out.Filename = objectstore.GoogleNativeExportFilename(meta.ContentType, meta.Filename)
				out.ContentType = exportMime
			}
			out.StorageURL = meta.StorageURL
			exp := now().Add(ttl)
			out.DownloadURL = signedObjectDownloadURL(signer, baseURL, in.StorageFileID, account, exp)
			out.ExpiresAt = exp.UTC().Format(time.RFC3339)
			out.Hint = downloadURLHint
			if in.Inline {
				ct := normalizedMIME(out.ContentType)
				switch {
				case !tool.IsMCPContext(ctx):
					out.Hint = "inline=true only attaches ImageContent over MCP; on the CLI use --save <path> instead. " + downloadURLHint
				case ct == "image/heic" || ct == "image/heif" || ct == "image/avif":
					out.Hint = ct + " cannot be inlined directly over MCP. If marts_screen_timelapse.active_screens.preview_storage_file_id is non-empty (or marts_photos.photos.thumbnail_storage_file_id), pass that id instead; if it is empty, run `pdw call get_object --data '{\"storage_file_id\":\"" + in.StorageFileID + "\"}' --save frame.png` on a shell or fetch download_url. " + downloadURLHint
				case !isInlineViewableImage(ct):
					out.Hint = "inline=true only attaches PNG, JPEG, WebP or GIF images; use download_url for " + out.ContentType + ". " + downloadURLHint
				case ct == "image/webp" && out.SizeBytes > maxInlineImageBytes:
					out.Hint = "WebP image exceeds the 4 MiB inline limit; pass preview_storage_file_id or fetch via download_url. " + downloadURLHint
				case out.SizeBytes > maxInlineRawFetchBytes:
					out.Hint = "Image exceeds the 32 MiB server resize limit; pass preview_storage_file_id / thumbnail_storage_file_id or fetch via download_url. " + downloadURLHint
				default:
					data, getErr := selectedStore.GetObject(ctx, ref)
					if getErr != nil {
						out.Error = getErr.Error()
						return out, nil
					}
					if imgBytes, imgMIME, reason, ok := prepareInlineImage(data, ct); ok {
						out.inlineImage = imgBytes
						out.inlineMIME = imgMIME
					} else if reason == "pixels" {
						out.Hint = "Image exceeds the 50 MP inline decode budget; pass preview_storage_file_id / thumbnail_storage_file_id or fetch via download_url. " + downloadURLHint
					} else {
						out.Hint = "Image exceeds the 4 MiB inline limit; pass preview_storage_file_id / thumbnail_storage_file_id or fetch via download_url. " + downloadURLHint
					}
				}
			}
			return out, nil
		},
		IsError: func(o getObjectOutput) bool { return o.Error != "" },
	}
}
