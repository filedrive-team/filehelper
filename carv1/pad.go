package carv1

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/filecoin-project/go-padreader"
	"github.com/ipfs/go-cid"
	gocar "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"golang.org/x/xerrors"
)

type NullReader struct{}

// Read writes NUL bytes into the provided byte slice.
func (nr NullReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

func PadCar(targetPath string) error {
	f, err := os.OpenFile(targetPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	var carSize = finfo.Size()

	br := bufio.NewReader(f)
	if _, err := gocar.ReadHeader(br); err != nil {
		return err
	}
	pieceSize := padreader.PaddedSize(uint64(carSize))
	if int64(pieceSize) == carSize {
		return nil
	}
	nr := io.LimitReader(NullReader{}, int64(pieceSize)-carSize)
	if _, err := f.Seek(carSize, 0); err != nil {
		return err
	}
	if _, err := io.Copy(f, nr); err != nil {
		return err
	}
	defer func() {
		if int64(pieceSize) > carSize {
			if strings.HasSuffix(targetPath, ".car") {
				if err := os.Rename(targetPath, strings.TrimSuffix(targetPath, ".car")); err != nil {
					fmt.Println(err)
				}
			}
		}
	}()
	return nil
}

func UnpadCar(targetPath string) error {
	f, err := os.OpenFile(targetPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	var carSize uint64

	br := bufio.NewReader(f)
	carHeader, err := gocar.ReadHeader(br)
	if err != nil {
		return err
	}
	headerSize, err := gocar.HeaderSize(carHeader)
	if err != nil {
		return err
	}
	carSize += headerSize
	cidmap := make(map[cid.Cid]int)
	for _, cid := range carHeader.Roots {
		cidmap[cid] = 0
	}
	for {
		if cid, d, err := carutil.ReadNode(br); err == nil {
			carSize += carutil.LdSize(d)
			cidmap[cid] = 1
		} else {
			if err != io.EOF {
				return err
			}
			break
		}
	}

	pieceSize := finfo.Size()
	fmt.Printf("car size: %d, piece size: %d\n", carSize, pieceSize)
	if pieceSize == int64(carSize) {
		return nil
	}
	missingCid := false
	for cid, v := range cidmap {
		if v == 0 {
			fmt.Printf("missing cid: %s\n", cid)
			missingCid = true
		}
	}
	if missingCid {
		return xerrors.Errorf("bad data, missing blocks")
	}
	if err := f.Truncate(int64(carSize)); err != nil {
		return err
	}
	defer func() {
		if pieceSize > int64(carSize) {
			if err := os.Rename(targetPath, targetPath+".car"); err != nil {
				fmt.Println(err)
			}
		}
	}()
	return nil
}
