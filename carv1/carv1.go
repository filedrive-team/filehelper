// helper package for building car file of version 1
package carv1

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	gocar "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"golang.org/x/xerrors"
)

type BatchBuilder struct {
	ctx context.Context
	bs  blockstore.Blockstore
}

func NewBatch(ctx context.Context, bs blockstore.Blockstore) *BatchBuilder {
	return &BatchBuilder{
		ctx: ctx,
		bs:  bs,
	}
}

func (b *BatchBuilder) WriteCar(root cid.Cid, outPath string, batchNum int) error {
	nd, err := GetNode(b.ctx, root, b.bs)
	if err != nil {
		return err
	}

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	// write header
	if err := gocar.WriteHeader(&gocar.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}, f); err != nil {
		return err
	}

	// write data
	// write root node
	if err := carutil.LdWrite(f, nd.Cid().Bytes(), nd.RawData()); err != nil {
		return err
	}
	//fmt.Printf("cid: %s\n", nd.Cid())
	if err := BlockWalk(b.ctx, nd, b.bs, batchNum, func(node format.Node) error {
		if err := carutil.LdWrite(f, node.Cid().Bytes(), node.RawData()); err != nil {
			return err
		}
		//fmt.Printf("cid: %s\n", node.Cid())
		return nil
	}); err != nil {
		return err
	}
	finfo, err := f.Stat()
	if err != nil {
		return err
	}
	carSize := finfo.Size()
	fmt.Printf("car file size: %d\n", carSize)

	return nil
}

func GetNode(ctx context.Context, cid cid.Cid, bs blockstore.Blockstore) (format.Node, error) {
	nd, err := bs.Get(cid)
	if err != nil {
		return nil, err
	}
	return legacy.DecodeNode(ctx, nd)
}

func BlockWalk(ctx context.Context, node format.Node, bs blockstore.Blockstore, batchNum int, cb func(nd format.Node) error) error {
	links := node.Links()
	if len(links) == 0 {
		return nil
	}
	loadedNode := make([]format.Node, len(links))
	errmsg := make([]string, 0)
	var wg sync.WaitGroup
	batchchan := make(chan struct{}, batchNum)
	wg.Add(len(links))
	for i, link := range links {
		go func(ctx context.Context, i int, link *format.Link, bs blockstore.Blockstore) {
			defer func() {
				<-batchchan
				wg.Done()
			}()
			batchchan <- struct{}{}
			nd, err := GetNode(ctx, link.Cid, bs)
			if err != nil {
				errmsg = append(errmsg, err.Error())
			}
			loadedNode[i] = nd
		}(ctx, i, link, bs)
	}
	wg.Wait()
	if len(errmsg) > 0 {
		return xerrors.New(strings.Join(errmsg, "\n"))
	}
	for _, nd := range loadedNode {
		if err := cb(nd); err != nil {
			return err
		}
		if err := BlockWalk(ctx, nd, bs, batchNum, cb); err != nil {
			return err
		}
	}
	return nil
}
