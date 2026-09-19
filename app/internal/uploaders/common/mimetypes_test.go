package common

import "testing"

func TestGuessMimeTypeMatchesPythonMimetypes(t *testing.T) {
	// Expected values from CPython 3.12 on macOS (with /etc/apache2/mime.types).
	cases := map[string]string{
		"x.HEIC":                       "image/heic",
		"a.tar.gz":                     "application/x-tar",
		"file.jpeg":                    "image/jpeg",
		"file.mpeg4":                   "",
		"noext":                        "",
		".bashrc":                      "",
		"~/Library/Messages/x/IMG.jpg": "image/jpeg",
		"voice.m4a":                    "audio/mp4a-latm",
		"notes.txt":                    "text/plain",
		"archive.tgz":                  "application/x-tar",
		"photo.PNG":                    "image/png",
		"doc.pdf":                      "application/pdf",
		"clip.mov":                     "video/quicktime",
	}
	for name, want := range cases {
		if got := GuessMimeType(name); got != want {
			t.Errorf("GuessMimeType(%q) = %q, want %q", name, got, want)
		}
	}
}
