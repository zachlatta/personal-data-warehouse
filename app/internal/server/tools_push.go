package server

import (
	"context"
	"time"

	pdwauth "github.com/zachlatta/personal-data-warehouse/app/internal/auth"
	"github.com/zachlatta/personal-data-warehouse/app/internal/push"
	"github.com/zachlatta/personal-data-warehouse/app/internal/tool"
)

const notifyDescription = "Send a push notification to Zach's phone (every device registered with the PDW iOS app). Use it for something that genuinely needs his attention now — a real person waiting on a reply, a deadline, a security alert — not for status. Fields map onto the iOS alert: title (required), subtitle, body, an https image_url shown as a thumbnail (or image_storage_file_id for a warehouse object, which becomes a signed link), category for action buttons (mutation_review: Approve/Deny/Review; link: Open; reply: inline text reply), route for the in-app screen a tap opens (/mutations/<id>, /timeline/<adapter>/<event_id>, /timeline, /search), thread_id to group related alerts, collapse_id to replace an earlier alert, interruption_level (passive, active, time-sensitive, critical), badge, and sound (default or none). Returns the delivery report per device."

type notifyInput struct {
	Title              string         `json:"title" jsonschema:"the alert title; keep it under ~40 characters"`
	Subtitle           string         `json:"subtitle,omitempty" jsonschema:"shown under the title in smaller type; who or what it is about"`
	Body               string         `json:"body,omitempty" jsonschema:"the alert text; iOS shows about four lines when expanded"`
	ImageURL           string         `json:"image_url,omitempty" jsonschema:"absolute https URL of an image to attach; the phone downloads it, so it must be publicly fetchable"`
	ImageStorageFileID string         `json:"image_storage_file_id,omitempty" jsonschema:"a warehouse storage_file_id (from get_object / storage_* columns) to attach instead of image_url; the app signs a download link for it"`
	ImageAccount       string         `json:"image_account,omitempty" jsonschema:"account for a google_drive_source image_storage_file_id"`
	Category           string         `json:"category,omitempty" jsonschema:"action buttons: mutation_review, link, or reply; omit for a plain alert"`
	Route              string         `json:"route,omitempty" jsonschema:"in-app path a tap opens, starting with /"`
	ThreadID           string         `json:"thread_id,omitempty" jsonschema:"groups alerts in Notification Center; e.g. mutations, digest, finance"`
	CollapseID         string         `json:"collapse_id,omitempty" jsonschema:"a newer alert with the same id replaces the older one"`
	InterruptionLevel  string         `json:"interruption_level,omitempty" jsonschema:"passive, active, time-sensitive, or critical"`
	Badge              *int           `json:"badge,omitempty" jsonschema:"app icon badge count; 0 clears it"`
	Sound              string         `json:"sound,omitempty" jsonschema:"default or none"`
	Data               map[string]any `json:"data,omitempty" jsonschema:"extra payload delivered to the app; request_id is what mutation actions read"`
}

type notifyOutput struct {
	Report   push.Report `json:"report"`
	ImageURL string      `json:"image_url,omitempty"`
	Error    string      `json:"error,omitempty"`
}

// notifyTool exposes the push notifier as a tool, so an agent (or `pdw call
// notify`) can send a rich alert without a code change. objectsAvailable says
// whether image_storage_file_id can be honored: the /objects/ download route
// only exists when object storage is configured.
func notifyTool(notifier *push.Notifier, signer *pdwauth.Service, baseURL string, ttl time.Duration, objectsAvailable bool, now func() time.Time) tool.Tool {
	if now == nil {
		now = time.Now
	}
	return &tool.Typed[notifyInput, notifyOutput]{
		NameStr:        "notify",
		TitleStr:       "Send Push Notification",
		DescriptionStr: notifyDescription,
		Handle: func(ctx context.Context, in notifyInput) (notifyOutput, error) {
			n := push.Notification{
				Title:             in.Title,
				Subtitle:          in.Subtitle,
				Body:              in.Body,
				ImageURL:          in.ImageURL,
				Category:          in.Category,
				Route:             in.Route,
				ThreadID:          in.ThreadID,
				CollapseID:        in.CollapseID,
				InterruptionLevel: in.InterruptionLevel,
				Badge:             in.Badge,
				Sound:             in.Sound,
				Data:              map[string]any{},
			}
			for k, v := range in.Data {
				n.Data[k] = v
			}
			if in.ImageStorageFileID != "" {
				if in.ImageURL != "" {
					return notifyOutput{}, &tool.InvalidInputError{Message: "pass image_url or image_storage_file_id, not both"}
				}
				if !objectsAvailable {
					return notifyOutput{}, &tool.InvalidInputError{Message: "image_storage_file_id needs object storage, which this deployment has not configured; pass image_url instead"}
				}
				n.ImageURL = signedObjectDownloadURL(signer, baseURL, in.ImageStorageFileID, in.ImageAccount, now().Add(ttl))
			}
			if err := n.Validate(); err != nil {
				return notifyOutput{}, &tool.InvalidInputError{Message: err.Error()}
			}
			n.Data["sent_by"] = pdwauth.ClientNameFromContext(ctx)
			report, err := notifier.Notify(ctx, n)
			if err != nil {
				return notifyOutput{Report: report, ImageURL: n.ImageURL, Error: err.Error()}, nil
			}
			return notifyOutput{Report: report, ImageURL: n.ImageURL}, nil
		},
		IsError: func(out notifyOutput) bool { return out.Error != "" },
	}
}
