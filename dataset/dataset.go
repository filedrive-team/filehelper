package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filehelper/importer"
	"github.com/filedrive-team/go-ds-cluster/clusterclient"
	clustercfg "github.com/filedrive-team/go-ds-cluster/config"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsmount "github.com/ipfs/go-datastore/mount"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
)

const record_json = "record.json"

type MetaData struct {
	Path string `json:"path"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	CID  string `json:"cid"`
}

var log = logging.Logger("filehelper/dataset")

func Import(ctx context.Context, target, dsclusterCfg string, parallel, batchReadNum int) error {
	recordPath := path.Join(target, record_json)
	// check if record.json has data
	records, err := readRecords(recordPath)
	if err != nil {
		return err
	}
	var ds ds.Datastore

	cfg, err := clustercfg.ReadConfig(dsclusterCfg)
	if err != nil {
		return err
	}
	ds, err = clusterclient.NewClusterClient(context.Background(), cfg)
	if err != nil {
		return err
	}

	ds = dsmount.New([]dsmount.Mount{
		{
			Prefix:    bstore.BlockPrefix,
			Datastore: ds,
		},
	})

	bs := bstore.NewBlockstore(ds.(*dsmount.Datastore))
	//bs = blockstore.NewParaBlockstore(bs, parallel*5)
	dagServ := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	// cidbuilder
	cidBuilder, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return err
	}

	var total_files = 0
	go func() {
		filepath.Walk(target, func(_ string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !fi.IsDir() && fi.Name() != record_json {
				total_files += 1
			}
			return nil
		})
	}()

	pchan := make(chan struct{}, parallel)
	wg := sync.WaitGroup{}
	lock := sync.RWMutex{}
	var ferr error
	files := filehelper.FileWalkAsync([]string{target})
	for item := range files {
		wg.Add(1)
		go func(item filehelper.Finfo) {
			defer func() {
				<-pchan
				wg.Done()
			}()
			pchan <- struct{}{}
			// ignore record_json
			if item.Name == record_json {
				return
			}

			// ignore file which has been imported
			lock.RLock()
			if _, ok := records[item.Path]; ok {
				lock.RUnlock()
				return
			}
			lock.RUnlock()

			fileNodeCid, err := buildFileNode(ctx, item, dagServ, cidBuilder, batchReadNum)
			if err != nil {
				ferr = err
				return
			}
			lock.Lock()
			defer lock.Unlock()
			records[item.Path] = &MetaData{
				Path: item.Path,
				Name: item.Name,
				Size: item.Info.Size(),
				CID:  fileNodeCid.String(),
			}
			if total_files > 0 {
				fmt.Printf("total %d files, imported %d files, %.2f %%\n", total_files, len(records), float64(len(records))/float64(total_files)*100)
			}
		}(item)
	}
	wg.Wait()
	err = saveRecords(records, recordPath)
	if err != nil {
		ferr = err
	}
	return ferr
}

func buildFileNode(ctx context.Context, item filehelper.Finfo, dagServ ipld.DAGService, cidBuilder cid.Builder, batchReadNum int) (root cid.Cid, err error) {
	f, err := os.Open(item.Path)
	if err != nil {
		return cid.Undef, err
	}
	defer f.Close()
	log.Infof("import file: %s", item.Path)
	rootcid, err := importer.BalanceNode(ctx, f, item.Info.Size(), dagServ, cidBuilder, batchReadNum)
	if err != nil {
		return cid.Undef, err
	}

	return rootcid, nil

}

func readRecords(path string) (map[string]*MetaData, error) {
	res := make(map[string]*MetaData)
	bs, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return res, nil
		}
		return nil, err
	}

	err = json.Unmarshal(bs, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func saveRecords(records map[string]*MetaData, path string) error {
	bs, err := json.Marshal(records)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, bs, 0666)
}
