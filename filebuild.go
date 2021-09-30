package filehelper

import (
	"io"
	"os"

	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"

	ipld "github.com/ipfs/go-ipld-format"
)

const UnixfsLinksPerLevel = 1 << 10
const UnixfsChunkSize uint64 = 1 << 20

func BuildFileNode(item Finfo, bufDs ipld.DAGService, cidBuilder cid.Builder) (node ipld.Node, err error) {
	var r io.Reader
	f, err := os.Open(item.Path)
	if err != nil {
		return nil, err
	}
	r = f

	// read all data of item
	if item.SeekStart > 0 || item.SeekEnd > 0 {
		r = &FileSlice{
			r:        f,
			start:    item.SeekStart,
			end:      item.SeekEnd,
			fileSize: item.Info.Size(),
		}
	}

	params := ihelper.DagBuilderParams{
		Maxlinks:   UnixfsLinksPerLevel,
		RawLeaves:  false,
		CidBuilder: cidBuilder,
		Dagserv:    bufDs,
		NoCopy:     false,
	}
	db, err := params.New(chunker.NewSizeSplitter(r, int64(UnixfsChunkSize)))
	if err != nil {
		return nil, err
	}
	node, err = balanced.Layout(db)
	if err != nil {
		return nil, err
	}
	return
}
