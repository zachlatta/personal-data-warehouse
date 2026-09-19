package common

import "strings"

// GuessMimeType mirrors Python's mimetypes.guess_type(filename)[0] as it
// behaved on the Macs the uploaders run on (CPython's default table overlaid
// with /etc/apache2/mime.types), returning "" when Python returned None. The
// Python table, not Go's platform-dependent mime package, is what the
// attachment content types in the fingerprinted payloads were computed with.
func GuessMimeType(filename string) string {
	base, ext := pySplitext(filename)
	for {
		mapped, ok := pyMimeSuffixMap[strings.ToLower(ext)]
		if !ok {
			break
		}
		base, ext = pySplitext(base + mapped)
	}
	if _, ok := pyMimeEncodingsMap[ext]; ok {
		base, ext = pySplitext(base)
	}
	_ = base
	return pyMimeTypesMap[strings.ToLower(ext)]
}

// pySplitext mirrors posixpath.splitext: the extension is the final
// component's last dot onward, unless every character before it is a dot.
func pySplitext(path string) (string, string) {
	sepIndex := strings.LastIndex(path, "/")
	dotIndex := strings.LastIndex(path, ".")
	if dotIndex <= sepIndex {
		return path, ""
	}
	filenameIndex := sepIndex + 1
	for filenameIndex < dotIndex {
		if path[filenameIndex] != '.' {
			return path[:dotIndex], path[dotIndex:]
		}
		filenameIndex++
	}
	return path, ""
}
