package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filehelper/importer"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"golang.org/x/xerrors"
)

const record_json = "record.json"
const record_csv = "record.csv"

type MetaData struct {
	Path string `json:"path"`
	Name string `json:"name"`
	Size int64  `json:"size"`
	CID  string `json:"cid"`
}

var log = logging.Logger("filehelper/dataset")

func Import(ctx context.Context, bs bstore.Blockstore, cidBuilder cid.Prefix, parallel, batchReadNum int, prefix, recordDir string, targets []string) error {
	// checkout if record dir exists
	rdinfo, err := os.Stat(recordDir)
	if err != nil {
		return err
	}
	if !rdinfo.IsDir() {
		return xerrors.New("record dir is not a dir!")
	}

	recordPath := path.Join(recordDir, record_json)
	// check if record.json has data
	records, err := readRecords(recordPath)
	if err != nil {
		return err
	}
	// set up a goroutine to receive csv record line by line
	recordCSVPath := path.Join(recordDir, record_csv)
	csvChan := make(chan string)
	defer close(csvChan)
	go func(ctx context.Context, csvPath string, csvChan chan string) {
		f, err := os.OpenFile(csvPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			// if we failed to open file handle then quit import should be a better choice
			panic(err)
		}
		defer f.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case record := <-csvChan:
				if record == "" { // chan closed
					return
				}
				if _, err := f.WriteString(record); err != nil {
					log.Error(err)
				}
			}
		}

	}(ctx, recordCSVPath, csvChan)

	dagServ := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	var total_files uint64 = 0
	var total_size uint64
	var importedSize uint64
	go func() {
		for _, target := range targets {
			filepath.Walk(target, func(_ string, fi os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if fi.Size() == 0 {
					return nil
				}
				if !fi.IsDir() {
					total_files += 1
					total_size += uint64(fi.Size())
				}
				return nil
			})
		}
	}()

	pchan := make(chan struct{}, parallel)
	wg := sync.WaitGroup{}
	lock := sync.RWMutex{}
	var ferr error
	files := filehelper.FileWalkAsync(targets)
	for item := range files {
		wg.Add(1)
		go func(item filehelper.Finfo) {
			defer func() {
				<-pchan
				wg.Done()
			}()
			pchan <- struct{}{}

			if item.Info.Size() == 0 {
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

			atomic.AddUint64(&importedSize, uint64(item.Info.Size()))

			if total_size > 0 {
				fmt.Printf("total %d files, imported %d files, %.2f %%\n", total_files, len(records), float64(len(records))/float64(total_files)*100)
				fmt.Printf("total size: %d, imported size: %d, %.2f %%\n", total_size, importedSize, float64(importedSize)/float64(total_size)*100)
			}
			csvChan <- fmt.Sprintf("%s,%s,%d\n", strings.TrimPrefix(item.Path, prefix), fileNodeCid.String(), item.Info.Size())
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
