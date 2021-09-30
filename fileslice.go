package filehelper

import (
	"io"
	"os"

	"golang.org/x/xerrors"
)

type FileSlice struct {
	r        *os.File
	offset   int64
	start    int64
	end      int64
	fileSize int64
}

func (fs *FileSlice) Read(p []byte) (n int, err error) {
	if fs.end == 0 {
		fs.end = fs.fileSize - 1
	}
	if fs.offset == 0 && fs.start > 0 {
		_, err = fs.r.Seek(fs.start, 0)
		if err != nil {
			return 0, err
		}
		fs.offset = fs.start
	}
	if fs.end-fs.offset+1 == 0 {
		return 0, io.EOF
	}
	if fs.end-fs.offset+1 < 0 {
		return 0, xerrors.Errorf("read data out bound of the slice")
	}
	plen := len(p)
	leftLen := fs.end - fs.offset + 1
	if leftLen > int64(plen) {
		n, err = fs.r.Read(p)
		if err != nil {
			return
		}
		fs.offset += int64(n)
		return
	}
	b := make([]byte, leftLen)
	n, err = fs.r.Read(b)
	if err != nil {
		return
	}
	fs.offset += int64(n)

	return copy(p, b), io.EOF
}
