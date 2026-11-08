package androidphotos

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
)

// MotionVideo is where a motion photo's video sits inside its JPEG: Pixel
// Camera appends the MP4 to the still and says so in the XMP, so the file
// MediaStore lists is both. The video goes to the warehouse as the photo's
// live_video rendition (the role an Apple Live Photo's .mov has).
type MotionVideo struct {
	Offset                  int64
	Length                  int64
	Format                  string // "motion_photo" (Container directory) or "micro_video" (older GCamera)
	PresentationTimestampUs int64
}

var (
	xmpContainerItem = regexp.MustCompile(`<Container:Item\b([^>]*?)/?>`)
	xmpItemAttribute = regexp.MustCompile(`Item:(\w+)="([^"]*)"`)
	xmpMicroVideo    = regexp.MustCompile(`GCamera:MicroVideo="1"`)
	xmpMicroOffset   = regexp.MustCompile(`GCamera:MicroVideoOffset="(\d+)"`)
	xmpPresentation  = regexp.MustCompile(`(?:GCamera:MotionPhotoPresentationTimestampUs|GCamera:MicroVideoPresentationTimestampUs)="(-?\d+)"`)
)

// FindMotionVideo reads where a motion photo's video is from the JPEG's XMP,
// for a file of fileSize bytes. ok is false for a still with no video.
//
// Motion Photo format 1 lists the file's items in order in a Container
// directory, each secondary item with its Length and the Padding that follows
// it; the items after the primary image are laid end to end up to the end of
// the file, so an item starts at the file size less its own length and
// padding and everything after it. Before that, GCamera:MicroVideoOffset gave
// the video's length counted back from the end of the file.
func FindMotionVideo(xmp []byte, fileSize int64) (MotionVideo, bool) {
	text := string(xmp)
	var presentation int64
	if m := xmpPresentation.FindStringSubmatch(text); m != nil {
		presentation, _ = strconv.ParseInt(m[1], 10, 64)
	}
	type item struct {
		semantic, mime  string
		length, padding int64
	}
	var items []item
	for _, element := range xmpContainerItem.FindAllStringSubmatch(text, -1) {
		var it item
		for _, attr := range xmpItemAttribute.FindAllStringSubmatch(element[1], -1) {
			switch attr[1] {
			case "Semantic":
				it.semantic = attr[2]
			case "Mime":
				it.mime = attr[2]
			case "Length":
				it.length, _ = strconv.ParseInt(attr[2], 10, 64)
			case "Padding":
				it.padding, _ = strconv.ParseInt(attr[2], 10, 64)
			}
		}
		items = append(items, it)
	}
	for k, it := range items {
		if it.semantic != "MotionPhoto" || it.length <= 0 {
			continue
		}
		end := fileSize - it.padding
		for _, after := range items[k+1:] {
			end -= after.length + after.padding
		}
		start := end - it.length
		if start <= 0 || end > fileSize {
			return MotionVideo{}, false
		}
		return MotionVideo{Offset: start, Length: it.length, Format: "motion_photo", PresentationTimestampUs: presentation}, true
	}
	if xmpMicroVideo.MatchString(text) {
		if m := xmpMicroOffset.FindStringSubmatch(text); m != nil {
			length, _ := strconv.ParseInt(m[1], 10, 64)
			if length > 0 && length < fileSize {
				return MotionVideo{Offset: fileSize - length, Length: length, Format: "micro_video", PresentationTimestampUs: presentation}, true
			}
		}
	}
	return MotionVideo{}, false
}

// ExtractMotionVideo copies a motion photo's video to dest and returns its
// sha256. The bytes must be an MP4 (an ISO BMFF "ftyp" box first): an XMP that
// points anywhere else is an error, never a video made of JPEG bytes.
func ExtractMotionVideo(path string, video MotionVideo, dest string) (string, error) {
	src, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer src.Close()
	var head [8]byte
	if _, err := src.ReadAt(head[:], video.Offset); err != nil {
		return "", fmt.Errorf("read motion video header: %w", err)
	}
	if string(head[4:8]) != "ftyp" {
		return "", fmt.Errorf("motion video at byte %d is not an MP4", video.Offset)
	}
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(out, hash), io.NewSectionReader(src, video.Offset, video.Length))
	closeErr := out.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if written != video.Length {
		return "", fmt.Errorf("motion video is %d bytes, XMP says %d", written, video.Length)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
