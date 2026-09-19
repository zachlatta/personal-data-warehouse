// Package applenotes uploads the Mac's Notes store (NoteStore.sqlite) as
// per-note revisions through the app's /ingest/apple-notes endpoints.
package applenotes

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"database/sql"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/zachlatta/personal-data-warehouse/app/internal/uploaders/common"
)

// DefaultStorePath is the Notes database.
const DefaultStorePath = "~/Library/Group Containers/group.com.apple.notes/NoteStore.sqlite"

var utiContentTypes = map[string]string{
	"com.adobe.pdf":             "application/pdf",
	"com.apple.quicktime-movie": "video/quicktime",
	"com.compuserve.gif":        "image/gif",
	"public.heic":               "image/heic",
	"public.html":               "text/html",
	"public.jpeg":               "image/jpeg",
	"public.mpeg-4":             "video/mp4",
	"public.mpeg-4-audio":       "audio/mp4",
	"public.mp3":                "audio/mpeg",
	"public.png":                "image/png",
	"public.plain-text":         "text/plain",
	"public.tiff":               "image/tiff",
}

// Attachment types that never have their own file on disk.
var noFileAttachmentErrors = map[string]string{
	"public.url":              "link attachment has no file; the URL is in raw metadata (ZURLSTRING)",
	"com.apple.notes.table":   "table attachment has no file; the table is embedded in the note body",
	"com.apple.notes.gallery": "gallery attachment has no file; its pages are separate attachments",
}

const (
	inlineAttachmentUTIPrefix = "com.apple.notes.inlinetextattachment"
	inlineAttachmentError     = "inline text attachment has no file; its text is in raw metadata (ZALTTEXT)"
)

// Attachment is one note attachment.
type Attachment struct {
	AttachmentID  string
	NoteID        string
	Filename      string
	ContentType   string
	Path          string // "" when no local file
	SizeBytes     int64
	ContentSHA256 string
	IsMissing     bool
	Error         string
	Raw           map[string]any
}

// Note is one note as the uploader ships it.
type Note struct {
	NoteID           string
	Title            string
	FolderID         string
	FolderPath       string
	AppleAccountID   string
	AppleAccountName string
	CreatedAt        time.Time
	ModifiedAt       time.Time
	BodyText         string
	BodyHTML         string
	BodyMarkdown     string
	Attachments      []Attachment
	IsDeleted        bool
	Raw              map[string]any
}

// SnapshotStore copies the live NoteStore.sqlite into destinationDir.
func SnapshotStore(storePath, destinationDir string) (string, error) {
	destination := filepath.Join(destinationDir, "NoteStore.sqlite")
	if err := common.SnapshotSQLite(common.ExpandUser(storePath), destination); err != nil {
		return "", err
	}
	return destination, nil
}

// Scan reads every note from a (snapshotted) store; attachmentsRoot is the
// group container that holds Accounts/*/Media etc.
func Scan(storePath, attachmentsRoot string) ([]Note, error) {
	db, err := common.OpenSQLite(storePath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tables, err := common.TableNames(db)
	if err != nil {
		return nil, err
	}
	root := ""
	if attachmentsRoot != "" {
		root = common.ExpandUser(attachmentsRoot)
	}
	var notes []Note
	switch {
	case tables["notes"]:
		notes, err = scanSynthetic(db, tables, root)
	case tables["ZICCLOUDSYNCINGOBJECT"] && tables["Z_PRIMARYKEY"]:
		notes, err = scanCoreData(db, tables, root)
	default:
		return nil, fmt.Errorf("Unsupported Apple Notes schema: expected a synthetic notes table or Apple's Core Data tables")
	}
	if err != nil {
		return nil, err
	}
	sort.SliceStable(notes, func(i, j int) bool {
		if !notes[i].ModifiedAt.Equal(notes[j].ModifiedAt) {
			return notes[i].ModifiedAt.Before(notes[j].ModifiedAt)
		}
		return notes[i].NoteID < notes[j].NoteID
	})
	return notes, nil
}

type folderInfo struct{ name, parent, account string }

func folderPath(folderID string, folders map[string]folderInfo) string {
	if folderID == "" {
		return ""
	}
	var parts []string
	seen := map[string]bool{}
	current := folderID
	for current != "" && !seen[current] {
		seen[current] = true
		folder, ok := folders[current]
		if !ok {
			break
		}
		if folder.name != "" {
			parts = append(parts, folder.name)
		}
		current = folder.parent
	}
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, "/")
}

func scanSynthetic(db *sql.DB, tables map[string]bool, root string) ([]Note, error) {
	folders := map[string]folderInfo{}
	if tables["folders"] {
		rows, err := common.SelectAll(db, "folders", false)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			folders[row.First("folder_id", "id")] = folderInfo{name: row.First("name", "title"), parent: row.First("parent_folder_id", "parent_id"), account: row.First("account_id")}
		}
	}
	accounts := map[string]string{}
	if tables["accounts"] {
		rows, err := common.SelectAll(db, "accounts", false)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			accounts[row.First("account_id", "id")] = row.First("name", "account_name", "identifier")
		}
	}
	attachmentsByNote := map[string][]Attachment{}
	if tables["attachments"] {
		rows, err := common.SelectAll(db, "attachments", false)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			noteID := row.First("note_id")
			if noteID == "" {
				continue
			}
			attachmentsByNote[noteID] = append(attachmentsByNote[noteID], attachmentFromRow(row, attachmentRowSpec{
				noteID:             noteID,
				root:               root,
				idColumns:          []string{"attachment_id", "id", "identifier"},
				pathColumns:        []string{"path", "file_path", "filename"},
				filenameColumns:    []string{"filename", "name"},
				contentTypeColumns: []string{"content_type", "mime_type", "uti"},
			}))
		}
	}
	rows, err := common.SelectAll(db, "notes", false)
	if err != nil {
		return nil, err
	}
	var notes []Note
	for _, row := range rows {
		noteID := row.First("note_id", "id", "identifier")
		if noteID == "" {
			continue
		}
		folderID := row.First("folder_id")
		accountID := row.First("account_id")
		if accountID == "" {
			accountID = folders[folderID].account
		}
		bodyHTML := row.First("body_html", "html")
		bodyText := row.First("body_text", "text", "plaintext")
		if bodyText == "" && bodyHTML != "" {
			bodyText = PlainTextFromHTML(bodyHTML)
		}
		bodyMarkdown := row.First("body_markdown", "markdown")
		if bodyMarkdown == "" {
			bodyMarkdown = markdownFromBody(bodyHTML, bodyText)
		}
		if bodyHTML == "" {
			bodyHTML = htmlFromText(bodyText)
		}
		accountName, ok := accounts[accountID]
		if !ok {
			accountName = accountID
		}
		notes = append(notes, Note{
			NoteID:           noteID,
			Title:            row.First("title", "name"),
			FolderID:         folderID,
			FolderPath:       folderPath(folderID, folders),
			AppleAccountID:   accountID,
			AppleAccountName: accountName,
			CreatedAt:        datetimeValue(row, "created_at", "creation_date"),
			ModifiedAt:       datetimeValue(row, "modified_at", "updated_at", "modification_date"),
			BodyText:         bodyText,
			BodyHTML:         bodyHTML,
			BodyMarkdown:     bodyMarkdown,
			Attachments:      attachmentsByNote[noteID],
			IsDeleted:        boolValue(row, "is_deleted", "deleted"),
			Raw:              row.Public(),
		})
	}
	return notes, nil
}

func datetimeValue(row common.Row, columns ...string) time.Time {
	if value, ok := row.FirstValue(columns...); ok {
		return common.NotesDatetime(value)
	}
	return common.UnixEpoch
}

func boolValue(row common.Row, columns ...string) bool {
	switch strings.ToLower(strings.TrimSpace(row.First(columns...))) {
	case "1", "true", "yes", "y":
		return true
	}
	return false
}

func intValue(row common.Row, columns ...string) int64 {
	return common.ToInt(row.First(columns...))
}

type mediaFile struct {
	path       string
	filename   string
	identifier string
}

func scanCoreData(db *sql.DB, tables map[string]bool, root string) ([]Note, error) {
	entityRows, err := common.Query(db, "SELECT Z_ENT, Z_NAME FROM Z_PRIMARYKEY")
	if err != nil {
		return nil, err
	}
	noteEntities, folderEntities, accountEntities, attachmentEntities, mediaEntities := map[int64]bool{}, map[int64]bool{}, map[int64]bool{}, map[int64]bool{}, map[int64]bool{}
	for _, row := range entityRows {
		if row.Get("Z_ENT") == nil || row.Get("Z_NAME") == nil {
			continue
		}
		id := row.Int("Z_ENT")
		name := strings.ToLower(row.String("Z_NAME"))
		if strings.Contains(name, "note") && !strings.Contains(name, "data") && !strings.Contains(name, "attachment") {
			noteEntities[id] = true
		}
		if strings.Contains(name, "folder") {
			folderEntities[id] = true
		}
		if strings.Contains(name, "account") {
			accountEntities[id] = true
		}
		if strings.Contains(name, "attachment") {
			attachmentEntities[id] = true
		}
		if strings.Contains(name, "media") {
			mediaEntities[id] = true
		}
	}
	if len(noteEntities) == 0 {
		return nil, fmt.Errorf("Unsupported Apple Notes schema: could not identify note entities")
	}
	objectRows, err := common.SelectAll(db, "ZICCLOUDSYNCINGOBJECT", false)
	if err != nil {
		return nil, err
	}
	folders := map[string]folderInfo{}
	accounts := map[string]string{}
	for _, row := range objectRows {
		ent := row.Int("Z_ENT")
		pk := row.String("Z_PK")
		if folderEntities[ent] {
			folders[pk] = folderInfo{
				name:    row.First("ZTITLE2", "ZTITLE1", "ZTITLE", "ZNAME", "ZIDENTIFIER"),
				parent:  row.First("ZPARENT", "ZPARENTFOLDER", "ZFOLDER"),
				account: row.First("ZACCOUNT", "ZACCOUNT1", "ZACCOUNT2", "ZACCOUNT3", "ZACCOUNT4"),
			}
		}
		if accountEntities[ent] {
			accounts[pk] = row.First("ZNAME", "ZACCOUNTNAME", "ZIDENTIFIER", "ZEMAILADDRESS")
		}
	}
	noteData, err := coreDataNoteData(db, tables)
	if err != nil {
		return nil, err
	}
	attachmentsByNote := coreDataAttachmentsByNote(objectRows, attachmentEntities, mediaEntities, root)
	var notes []Note
	for _, row := range objectRows {
		if !noteEntities[row.Int("Z_ENT")] {
			continue
		}
		noteID := row.First("ZIDENTIFIER", "ZUNIQUEIDENTIFIER", "ZUUID", "ZGCKEY")
		if noteID == "" {
			noteID = row.String("Z_PK")
		}
		folderID := row.First("ZFOLDER", "ZFOLDER1", "ZPARENT", "ZPARENTFOLDER")
		accountID := row.First("ZACCOUNT", "ZACCOUNT1", "ZACCOUNT2", "ZACCOUNT3", "ZACCOUNT4")
		bodyText, bodyHTML := coreDataBody(row, noteData)
		title := row.First("ZTITLE1", "ZTITLE", "ZNAME", "ZSNIPPET")
		bodyMarkdown := markdownFromBody(bodyHTML, bodyText)
		var attachments []Attachment
		for _, attachment := range attachmentsByNote[row.String("Z_PK")] {
			attachment.NoteID = noteID
			attachments = append(attachments, attachment)
		}
		if bodyHTML == "" {
			bodyHTML = htmlFromText(bodyText)
		}
		accountName, ok := accounts[accountID]
		if !ok {
			accountName = accountID
		}
		notes = append(notes, Note{
			NoteID:           noteID,
			Title:            title,
			FolderID:         folderID,
			FolderPath:       folderPath(folderID, folders),
			AppleAccountID:   accountID,
			AppleAccountName: accountName,
			CreatedAt:        datetimeValue(row, "ZCREATIONDATE3", "ZCREATIONDATE1", "ZCREATIONDATE", "ZCREATEDDATE"),
			ModifiedAt:       datetimeValue(row, "ZMODIFICATIONDATE1", "ZMODIFICATIONDATE", "ZUPDATEDDATE"),
			BodyText:         bodyText,
			BodyHTML:         bodyHTML,
			BodyMarkdown:     bodyMarkdown,
			Attachments:      attachments,
			IsDeleted:        boolValue(row, "ZMARKEDFORDELETION", "ZISDELETED", "ZDELETED"),
			Raw:              row.Public(),
		})
	}
	return notes, nil
}

func coreDataNoteData(db *sql.DB, tables map[string]bool) (map[string][2]string, error) {
	out := map[string][2]string{}
	var dataTables []string
	for table := range tables {
		lower := strings.ToLower(table)
		if lower == "zicnotedata" || lower == "notedata" {
			dataTables = append(dataTables, table)
		}
	}
	sort.Strings(dataTables)
	for _, table := range dataTables {
		rows, err := common.SelectAll(db, table, false)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			dataPK := row.First("Z_PK", "id")
			if dataPK == "" {
				continue
			}
			decoded := ""
			for _, column := range row.Columns {
				if blob, ok := row.Values[column].([]byte); ok {
					decoded = DecodeNoteBlob(blob)
					if decoded != "" {
						break
					}
				}
			}
			bodyHTML := ""
			if strings.Contains(decoded, "<") && strings.Contains(decoded, ">") {
				bodyHTML = decoded
			}
			bodyText := decoded
			if bodyHTML != "" {
				bodyText = PlainTextFromHTML(bodyHTML)
			}
			out[dataPK] = [2]string{bodyText, bodyHTML}
		}
	}
	return out, nil
}

func coreDataBody(row common.Row, noteData map[string][2]string) (string, string) {
	text := row.First("ZBODY", "ZBODYTEXT", "ZPLAINTEXT", "ZSNIPPET", "ZSUMMARY", "ZTITLE1")
	html := row.First("ZHTML", "ZBODYHTML", "ZHTMLSTRING")
	if data, ok := noteData[row.First("ZNOTEDATA", "ZNOTEDATA1", "ZDATA")]; ok {
		if data[0] != "" {
			text = data[0]
		}
		if data[1] != "" {
			html = data[1]
		}
	}
	return text, html
}

func coreDataAttachmentsByNote(rows []common.Row, attachmentEntities, mediaEntities map[int64]bool, root string) map[string][]Attachment {
	out := map[string][]Attachment{}
	if len(attachmentEntities) == 0 {
		return out
	}
	media := coreDataMediaFiles(rows, mediaEntities, root)
	for _, row := range rows {
		if !attachmentEntities[row.Int("Z_ENT")] {
			continue
		}
		notePK := row.First("ZNOTE", "ZNOTE1", "ZOWNER", "ZPARENT")
		if notePK == "" {
			continue
		}
		spec := attachmentRowSpec{
			noteID:             notePK,
			root:               root,
			idColumns:          []string{"ZIDENTIFIER", "ZUUID", "Z_PK"},
			pathColumns:        []string{"ZFILEURL", "ZURL", "ZPATH", "ZFILENAME", "ZTITLE"},
			filenameColumns:    []string{"ZFILENAME", "ZTITLE", "ZNAME"},
			contentTypeColumns: []string{"ZMIMETYPE", "ZTYPEUTI", "ZUTI", "ZCONTENTTYPE"},
		}
		if m, ok := media[row.First("ZMEDIA")]; ok {
			spec.pathOverride = m.path
			spec.filenameOverride = m.filename
			spec.rawExtra = map[string]any{"ZMEDIA_IDENTIFIER": m.identifier, "ZMEDIA_FILENAME": m.filename}
		}
		if spec.pathOverride == "" {
			if fallback := resolveFallbackPath(row, root); fallback != "" {
				spec.pathOverride = fallback
				spec.filenameOverride = fallbackAttachmentFilename(row, fallback)
			}
		}
		if spec.pathOverride == "" {
			spec.missingError = noFileAttachmentError(row)
		}
		out[notePK] = append(out[notePK], attachmentFromRow(row, spec))
	}
	return out
}

func coreDataMediaFiles(rows []common.Row, mediaEntities map[int64]bool, root string) map[string]mediaFile {
	out := map[string]mediaFile{}
	if len(mediaEntities) == 0 {
		return out
	}
	for _, row := range rows {
		if !mediaEntities[row.Int("Z_ENT")] {
			continue
		}
		mediaPK := row.First("Z_PK")
		if mediaPK == "" {
			continue
		}
		identifier := row.First("ZIDENTIFIER", "ZUUID")
		filename := row.First("ZFILENAME", "ZTITLE", "ZNAME")
		path := resolveMediaPath(root, identifier, filename)
		name := filename
		if name == "" {
			if path != "" {
				name = filepath.Base(path)
			} else {
				name = identifier
			}
		}
		out[mediaPK] = mediaFile{path: path, filename: name, identifier: identifier}
	}
	return out
}

func accountAssetDirs(root, name string) []string {
	if root == "" {
		return nil
	}
	var dirs []string
	matches, _ := filepath.Glob(filepath.Join(root, "Accounts", "*", name))
	sort.Strings(matches)
	for _, match := range matches {
		if info, err := os.Stat(match); err == nil && info.IsDir() {
			dirs = append(dirs, match)
		}
	}
	direct := filepath.Join(root, name)
	if info, err := os.Stat(direct); err == nil && info.IsDir() {
		dirs = append(dirs, direct)
	}
	return dirs
}

func filesUnder(dir string) []string {
	var files []string
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil && info.Mode().IsRegular() {
				files = append(files, path)
			}
		}
		return nil
	})
	sort.Strings(files)
	return files
}

func resolveMediaPath(root, identifier, filename string) string {
	if identifier == "" {
		return ""
	}
	for _, mediaRoot := range accountAssetDirs(root, "Media") {
		mediaDir := filepath.Join(mediaRoot, identifier)
		if info, err := os.Stat(mediaDir); err != nil || !info.IsDir() {
			continue
		}
		files := filesUnder(mediaDir)
		if filename != "" {
			for _, candidate := range files {
				if filepath.Base(candidate) == filename {
					return candidate
				}
			}
		}
		if len(files) == 1 {
			return files[0]
		}
	}
	return ""
}

func resolveFallbackPath(row common.Row, root string) string {
	identifier := row.First("ZIDENTIFIER", "ZUUID")
	if identifier == "" {
		return ""
	}
	for _, spec := range [][2]string{{"FallbackImages", "ZFALLBACKIMAGEGENERATION"}, {"FallbackPDFs", "ZFALLBACKPDFGENERATION"}} {
		generation := row.First(spec[1])
		if generation == "" {
			continue
		}
		for _, fallbackRoot := range accountAssetDirs(root, spec[0]) {
			generationDir := filepath.Join(fallbackRoot, identifier, generation)
			if info, err := os.Stat(generationDir); err != nil || !info.IsDir() {
				continue
			}
			files := filesUnder(generationDir)
			if len(files) == 1 {
				return files[0]
			}
		}
	}
	return ""
}

func fallbackAttachmentFilename(row common.Row, path string) string {
	base := strings.TrimSpace(row.First("ZFILENAME", "ZTITLE", "ZNAME"))
	if strings.Trim(base, ".") == "" {
		base = row.First("ZIDENTIFIER", "ZUUID")
		if base == "" {
			base = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		}
	}
	ext := filepath.Ext(path)
	if ext != "" && !strings.HasSuffix(strings.ToLower(base), strings.ToLower(ext)) {
		base += ext
	}
	return base
}

func noFileAttachmentError(row common.Row) string {
	uti := row.First("ZTYPEUTI", "ZTYPEUTI1")
	if message, ok := noFileAttachmentErrors[uti]; ok {
		return message
	}
	if strings.HasPrefix(uti, inlineAttachmentUTIPrefix) {
		return inlineAttachmentError
	}
	return ""
}

type attachmentRowSpec struct {
	noteID             string
	root               string
	idColumns          []string
	pathColumns        []string
	filenameColumns    []string
	contentTypeColumns []string
	pathOverride       string
	filenameOverride   string
	missingError       string
	rawExtra           map[string]any
}

func attachmentFromRow(row common.Row, spec attachmentRowSpec) Attachment {
	attachmentID := row.First(spec.idColumns...)
	if attachmentID == "" {
		attachmentID = row.First("Z_PK")
	}
	if attachmentID == "" {
		attachmentID = "attachment"
	}
	rawPath := row.First(spec.pathColumns...)
	path := spec.pathOverride
	if path == "" {
		path = resolveAttachmentPath(rawPath, spec.root)
	}
	filename := spec.filenameOverride
	if filename == "" {
		filename = row.First(spec.filenameColumns...)
	}
	if filename == "" && path != "" {
		filename = filepath.Base(path)
	}
	if filename == "" && rawPath != "" {
		filename = filepath.Base(rawPath)
	}
	if filename == "" {
		filename = attachmentID
	}
	errText := row.First("error", "ZERROR")
	contentSHA := row.First("content_sha256", "ZCONTENTSHA256")
	size := intValue(row, "size_bytes", "size", "ZSIZE")
	isMissing := boolValue(row, "is_missing", "missing")
	exists := false
	if path != "" {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			exists = true
			size = info.Size()
			if contentSHA == "" {
				if sha, err := common.FileSHA256(path); err == nil {
					contentSHA = sha
				}
			}
			isMissing = false
		}
	}
	if !exists {
		if spec.missingError != "" {
			isMissing = false
			if errText == "" {
				errText = spec.missingError
			}
		} else if !isMissing {
			isMissing = true
			if errText == "" {
				errText = "attachment file is not locally available"
			}
		}
	}
	resolved := ""
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			resolved = path
		}
	}
	raw := row.Public()
	for key, value := range spec.rawExtra {
		raw[key] = value
	}
	return Attachment{
		AttachmentID:  attachmentID,
		NoteID:        spec.noteID,
		Filename:      filename,
		ContentType:   NormalizedContentType(row.First(spec.contentTypeColumns...), filename),
		Path:          resolved,
		SizeBytes:     size,
		ContentSHA256: contentSHA,
		IsMissing:     isMissing,
		Error:         errText,
		Raw:           raw,
	}
}

func resolveAttachmentPath(value, root string) string {
	if value == "" {
		return ""
	}
	if filepath.IsAbs(value) {
		return value
	}
	if root != "" {
		return filepath.Join(root, value)
	}
	return value
}

// NormalizedContentType mirrors the scanner's normalized_content_type.
func NormalizedContentType(value, filename string) string {
	contentType := strings.TrimSpace(value)
	if strings.Contains(contentType, "/") {
		return contentType
	}
	guessed := "application/octet-stream"
	if filename != "" {
		if g := common.GuessMimeType(filename); g != "" {
			guessed = g
		}
	}
	if guessed != "application/octet-stream" {
		return guessed
	}
	if mapped, ok := utiContentTypes[contentType]; ok {
		return mapped
	}
	if strings.HasPrefix(contentType, "public.") {
		extension := strings.ReplaceAll(strings.TrimPrefix(contentType, "public."), "-", "")
		if g := common.GuessMimeType("file." + extension); g != "" {
			return g
		}
	}
	return "application/octet-stream"
}

func markdownFromBody(bodyHTML, bodyText string) string {
	if bodyHTML != "" {
		return strings.TrimSpace(HTMLToMarkdown(bodyHTML))
	}
	return strings.TrimSpace(bodyText)
}

func htmlFromText(value string) string {
	if value == "" {
		return ""
	}
	return "<html><body><pre>" + PyHTMLEscape(value) + "</pre></body></html>"
}

// --- note body blobs --------------------------------------------------------

// DecodeNoteBlob mirrors _decode_note_blob: the gzipped protobuf note text
// when present, else the most readable text decoding of the payload.
func DecodeNoteBlob(value []byte) string {
	if text := decodeNoteProtobufText(value); text != "" {
		return text
	}
	var candidates []string
	for _, candidate := range noteBlobPayloadCandidates(value) {
		if decoded, ok := decodeText(candidate); ok {
			candidates = append(candidates, decoded)
		}
	}
	if len(candidates) > 0 {
		best := candidates[0]
		for _, candidate := range candidates[1:] {
			if textQualityBetter(candidate, best) {
				best = candidate
			}
		}
		if textIsReadable(best) {
			return best
		}
		return ""
	}
	var printable []byte
	for _, b := range value {
		if (b >= 32 && b <= 126) || b == 9 || b == 10 || b == 13 {
			printable = append(printable, b)
		}
	}
	total := len(value)
	if total == 0 {
		total = 1
	}
	if float64(len(printable))/float64(total) < 0.8 {
		return ""
	}
	return string(printable)
}

func decodeText(candidate []byte) (string, bool) {
	for _, encoding := range []string{"utf-8", "utf-16", "latin-1"} {
		var decoded string
		var ok bool
		switch encoding {
		case "utf-8":
			if utf8.Valid(candidate) {
				decoded, ok = string(candidate), true
			}
		case "utf-16":
			decoded, ok = decodeUTF16(candidate)
		case "latin-1":
			runes := make([]rune, len(candidate))
			for i, b := range candidate {
				runes[i] = rune(b)
			}
			decoded, ok = string(runes), true
		}
		if !ok {
			continue
		}
		cleaned := strings.ReplaceAll(decoded, "\x00", "")
		if strings.TrimSpace(cleaned) != "" {
			return cleaned, true
		}
		break
	}
	return "", false
}

// decodeUTF16 mirrors Python's "utf-16" codec: a BOM selects the byte order,
// otherwise native (little) endian; odd lengths and unpaired surrogates fail.
func decodeUTF16(data []byte) (string, bool) {
	if len(data)%2 != 0 {
		return "", false
	}
	little := true
	if len(data) >= 2 {
		if data[0] == 0xFF && data[1] == 0xFE {
			data = data[2:]
		} else if data[0] == 0xFE && data[1] == 0xFF {
			little = false
			data = data[2:]
		}
	}
	units := make([]uint16, len(data)/2)
	for i := range units {
		if little {
			units[i] = uint16(data[2*i]) | uint16(data[2*i+1])<<8
		} else {
			units[i] = uint16(data[2*i])<<8 | uint16(data[2*i+1])
		}
	}
	for i := 0; i < len(units); i++ {
		u := units[i]
		if u >= 0xD800 && u < 0xDC00 {
			if i+1 >= len(units) || units[i+1] < 0xDC00 || units[i+1] >= 0xE000 {
				return "", false
			}
			i++
		} else if u >= 0xDC00 && u < 0xE000 {
			return "", false
		}
	}
	return string(utf16.Decode(units)), true
}

func noteBlobPayloadCandidates(value []byte) [][]byte {
	var candidates [][]byte
	if reader, err := gzip.NewReader(bytes.NewReader(value)); err == nil {
		if data, err := io.ReadAll(reader); err == nil {
			candidates = append(candidates, data)
		}
	}
	if reader, err := zlib.NewReader(bytes.NewReader(value)); err == nil {
		if data, err := io.ReadAll(reader); err == nil {
			candidates = append(candidates, data)
		}
	}
	candidates = append(candidates, value)
	var deduped [][]byte
	for _, candidate := range candidates {
		duplicate := false
		for _, seen := range deduped {
			if bytes.Equal(seen, candidate) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			deduped = append(deduped, candidate)
		}
	}
	return deduped
}

func decodeNoteProtobufText(value []byte) string {
	for _, candidate := range noteBlobPayloadCandidates(value) {
		if text := extractNoteStoreProtobufText(candidate); text != "" {
			return text
		}
	}
	return ""
}

// extractNoteStoreProtobufText walks NoteStoreProto.document (2) ->
// Document.note (3) -> Note.note_text (2) and returns the longest text.
func extractNoteStoreProtobufText(payload []byte) string {
	for _, document := range protobufLengthDelimited(payload, 2) {
		for _, note := range protobufLengthDelimited(document, 3) {
			best := ""
			for _, field := range protobufLengthDelimited(note, 2) {
				if len(field) == 0 {
					continue
				}
				text := string(bytes.ToValidUTF8(field, []byte("�")))
				if strings.TrimSpace(text) == "" {
					continue
				}
				if len(text) > len(best) {
					best = text
				}
			}
			if best != "" {
				return best
			}
		}
	}
	return ""
}

func protobufLengthDelimited(payload []byte, fieldNumber int) [][]byte {
	var values [][]byte
	offset := 0
	for offset < len(payload) {
		key, next, ok := readVarint(payload, offset)
		if !ok {
			return values
		}
		offset = next
		number := int(key >> 3)
		wireType := key & 7
		switch wireType {
		case 0:
			_, next, ok := readVarint(payload, offset)
			if !ok {
				return values
			}
			offset = next
		case 1:
			offset += 8
		case 2:
			size, next, ok := readVarint(payload, offset)
			if !ok {
				return values
			}
			offset = next
			end := offset + int(size)
			if end > len(payload) || end < offset {
				return values
			}
			if number == fieldNumber {
				values = append(values, payload[offset:end])
			}
			offset = end
		case 5:
			offset += 4
		default:
			return values
		}
		if offset > len(payload) {
			return values
		}
	}
	return values
}

func readVarint(payload []byte, offset int) (uint64, int, bool) {
	var result uint64
	shift := 0
	for offset < len(payload) {
		b := payload[offset]
		offset++
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, offset, true
		}
		shift += 7
		if shift > 70 {
			return 0, offset, false
		}
	}
	return 0, offset, false
}

func textQuality(value string) (float64, int, int) {
	if value == "" {
		return 0, 0, 0
	}
	printable, weird := 0, 0
	total := 0
	for _, r := range value {
		total++
		if unicode.IsPrint(r) || r == '\n' || r == '\r' || r == '\t' {
			printable++
		}
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			weird++
		}
	}
	return float64(printable) / float64(total), -weird, total
}

func textQualityBetter(candidate, best string) bool {
	cp, cw, cl := textQuality(candidate)
	bp, bw, bl := textQuality(best)
	if cp != bp {
		return cp > bp
	}
	if cw != bw {
		return cw > bw
	}
	return cl > bl
}

func textIsReadable(value string) bool {
	if strings.TrimSpace(value) == "" {
		return false
	}
	ratio, negWeird, _ := textQuality(value)
	return ratio >= 0.95 && negWeird == 0
}
