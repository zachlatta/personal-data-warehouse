//go:build !unix

package agentsessions

import "io/fs"

func fileInode(fs.FileInfo) uint64 { return 0 }
