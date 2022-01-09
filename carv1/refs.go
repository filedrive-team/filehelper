package carv1

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	gocar "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
)

type RefType int8

const (
	RefHeader RefType = iota
	RefData
)

func (b *BatchBuilder) Ref(root cid.Cid, batchNum int) (*Carv1Ref, error) {
	nd, err := b.bs.Get(b.ctx, root)
	if err != nil {
		return nil, err
	}
	ref := &Carv1Ref{}
	ref.DataRef = make([]*DataRef, 0)
	var offset uint64

	h := &gocar.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}
	hz, err := gocar.HeaderSize(h)
	if err != nil {
		return nil, err
	}
	ref.Size += hz
	ref.DataRef = append(ref.DataRef, &DataRef{
		Offset: offset,
		Size:   hz,
		Type:   RefHeader,
		Blocks: []string{root.String()},
	})
	offset += hz
	// set cid set to only save uniq cid to car file
	cidSet := cid.NewSet()
	cidSet.Add(nd.Cid())

	// ref root node
	rootSize := carutil.LdSize(nd.Cid().Bytes(), nd.RawData())
	ref.Size += rootSize
	ref.DataRef = append(ref.DataRef, &DataRef{
		Offset: offset,
		Size:   rootSize,
		Type:   RefData,
		Block:  root.String(),
	})
	offset += rootSize
	//fmt.Printf("cid: %s\n", nd.Cid())
	if err := BlockWalk(b.ctx, nd, b.bs, batchNum, func(node format.Node) error {
		if cidSet.Has(node.Cid()) {
			return nil
		}

		cidSet.Add(node.Cid())
		bsize := carutil.LdSize(node.Cid().Bytes(), node.RawData())
		ref.Size += bsize
		ref.DataRef = append(ref.DataRef, &DataRef{
			Offset: bsize,
			Size:   bsize,
			Type:   RefData,
			Block:  node.Cid().String(),
		})
		offset += bsize
		return nil
	}); err != nil {
		return nil, err
	}

	fmt.Printf("car file size: %d\n", ref.Size)

	return ref, nil
}

type Carv1Ref struct {
	Size    uint64
	DataRef []*DataRef
}

type DataRef struct {
	Offset uint64
	Size   uint64
	Type   RefType
	Blocks []string
	Block  string
}

func (cr *Carv1Ref) Encode() ([]byte, error) {
	return cbor.Marshal(cr)
}

func DecodeCarv1Ref(data []byte) (ref *Carv1Ref, err error) {
	ref = &Carv1Ref{}
	err = cbor.Unmarshal(data, ref)
	return
}
