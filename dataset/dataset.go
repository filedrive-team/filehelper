package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/go-ds-cluster/clusterclient"
	clustercfg "github.com/filedrive-team/go-ds-cluster/config"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsmount "github.com/ipfs/go-datastore/mount"
	dss "github.com/ipfs/go-datastore/sync"
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

func Import(ctx context.Context, target, dsclusterCfg string, retry int, retryWait int, parallel int) error {
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

	bs2 := bstore.NewBlockstore(dss.MutexWrap(ds))
	dagServ := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

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

			fileNode, err := buildFileNodeRetry(retry, retryWait, item, dagServ, cidBuilder)
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
				CID:  fileNode.Cid().String(),
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

func buildFileNodeRetry(times, waitTime int, item filehelper.Finfo, dagServ ipld.DAGService, cidBuilder cid.Builder) (root ipld.Node, err error) {
	for i := 0; i <= times; i++ {
		log.Infof("import file: %s, try times: %d", item.Path, i)
		if root, err = filehelper.BuildFileNode(item, dagServ, cidBuilder); err == nil {
			return root, nil
		}
		// should wait a second if io.EOF
		if err == io.EOF {
			log.Infof("io.EOF wait %d seconds", waitTime)
			time.Sleep(time.Duration(waitTime * 1e9))
		}
	}
	return
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
