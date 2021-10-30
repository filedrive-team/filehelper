// customized blockstore for go-ds-cluster
//
// parallelize batch jobs

package blockstore

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"golang.org/x/xerrors"
)

var _ bstore.Blockstore = (*parablockstore)(nil)

func NewParaBlockstore(s bstore.Blockstore, parallel int) bstore.Blockstore {
	return &parablockstore{
		s: s,
		p: parallel,
	}
}

type parablockstore struct {
	s bstore.Blockstore
	p int
}

func (bs *parablockstore) HashOnRead(enabled bool) {
	bs.s.HashOnRead(enabled)
}

func (bs *parablockstore) Get(k cid.Cid) (blocks.Block, error) {
	return bs.s.Get(k)
}

func (bs *parablockstore) Put(block blocks.Block) error {
	return bs.s.Put(block)
}

func (bs *parablockstore) PutMany(bls []blocks.Block) error {
	//return bs.s.PutMany(blocks)
	pchan := make(chan struct{}, bs.p)
	wg := sync.WaitGroup{}

	var merr = make([]error, 0)
	for _, b := range bls {
		wg.Add(1)
		go func(b blocks.Block) {
			defer func() {
				<-pchan
				wg.Done()
			}()
			pchan <- struct{}{}
			err := bs.s.Put(b)
			if err != nil {
				merr = append(merr, err)
			}
		}(b)
	}
	wg.Wait()
	if len(merr) > 0 {
		var msg string
		for _, e := range merr {
			msg += e.Error() + "\n"
		}
		return xerrors.New(msg)
	}
	return nil
}

func (bs *parablockstore) Has(k cid.Cid) (bool, error) {
	return bs.s.Has(k)
}

func (bs *parablockstore) GetSize(k cid.Cid) (int, error) {
	return bs.s.GetSize(k)
}

func (bs *parablockstore) DeleteBlock(k cid.Cid) error {
	return bs.s.DeleteBlock(k)
}

func (bs *parablockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return bs.s.AllKeysChan(ctx)
}
