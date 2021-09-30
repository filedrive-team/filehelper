package filehelper

import (
	"os"
)

type Finfo struct {
	Path      string
	Name      string
	Info      os.FileInfo
	SeekStart int64
	SeekEnd   int64
}
