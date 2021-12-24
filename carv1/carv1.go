// helper package for building car file of version 1
package carv1

import (
	"context"
	"fmt"
	"io"
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

func (b *BatchBuilder) WriteToFile(root cid.Cid, outPath string, batchNum int) error {
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = b.Write(root, f, batchNum)
	return err
}

func (b *BatchBuilder) Write(root cid.Cid, w io.Writer, batchNum int) (uint64, error) {
	nd, err := GetNode(b.ctx, root, b.bs)
	if err != nil {
		return 0, err
	}
	w = &sw{w: w}
	var carSize uint64
	h := &gocar.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}

	// write header
	if err := gocar.WriteHeader(h, w); err != nil {
		return 0, err
	}
	if hz, err := gocar.HeaderSize(h); err != nil {
		return 0, err
	} else {
		carSize += hz
	}

	// write data
	// write root node
	if err := carutil.LdWrite(w, nd.Cid().Bytes(), nd.RawData()); err != nil {
		return 0, err
	}
	carSize += carutil.LdSize(nd.Cid().Bytes(), nd.RawData())
	//fmt.Printf("cid: %s\n", nd.Cid())
	if err := BlockWalk(b.ctx, nd, b.bs, batchNum, func(node format.Node) error {
		if err := carutil.LdWrite(w, node.Cid().Bytes(), node.RawData()); err != nil {
			return err
		}
		carSize += carutil.LdSize(nd.Cid().Bytes(), nd.RawData())
		//fmt.Printf("cid: %s\n", node.Cid())
		return nil
	}); err != nil {
		return 0, err
	}

	fmt.Printf("car file size: %d, write size: %d\n", carSize, w.(*sw).N())

	return carSize, nil
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

type sw struct {
	w io.Writer
	n uint64
}

func (w *sw) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	if err == nil {
		w.n += uint64(n)
	}
	return
}

func (w *sw) N() uint64 {
	return w.n
}
