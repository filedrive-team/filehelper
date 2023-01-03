package dataset

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filehelper/importer"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
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

// Bucket,Region,Key,Size,
func ImportV1(ctx context.Context, bs bstore.Blockstore, cidBuilder cid.Prefix, parallel, batchReadNum int, recordDir string, targets []string) error {
	// checkout if record dir exists
	rdinfo, err := os.Stat(recordDir)
	if err != nil {
		return err
	}
	if !rdinfo.IsDir() {
		return xerrors.New("record dir is not a dir!")
	}

	dagServ := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	var totalFiles uint64 = 0
	var totalSize uint64
	var importedSize uint64
	var importedFiles uint64
	go func() {
		for _, target := range targets {
			recordCSVPath := path.Join(recordDir, path.Base(target))
			data := filehelper.ReadCsv(target)
			recordData := filehelper.ReadCsvNoTitle(recordCSVPath)
			importedFiles = uint64(len(recordData))
			for _, v := range recordData {
				if i, err := strconv.Atoi(v[2]); err == nil {
					importedSize += uint64(i)
				}
			}
			for _, v := range data {
				if i, err := strconv.Atoi(v[3]); err == nil {
					totalFiles += 1
					totalSize += uint64(i)
				}
			}
		}
	}()

	pchan := make(chan struct{}, parallel)
	wg := sync.WaitGroup{}
	lock := sync.RWMutex{}
	var ferr error

	for _, target := range targets {
		csvChan := make(chan string)
		fileDownloaderChan := make(chan FileDownloader)
		fileDownloaderErrorChan := make(chan struct{})

		shutdownChan := make(chan os.Signal)

		signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)
		defer close(shutdownChan)
		defer close(csvChan)

		filenameall := path.Base(target)
		filesuffix := path.Ext(target)
		fileprefix := filenameall[0 : len(filenameall)-len(filesuffix)]

		recordPath := path.Join(recordDir, fileprefix+".json")
		recordErrorPath := path.Join(recordDir, fileprefix+"error.json")
		downloaderRecord, err := readDownloader(recordErrorPath)
		if err != nil {
			return err
		}
		// check if record.json has data
		records, err := readRecords(recordPath)
		if err != nil {
			return err
		}
		// set up a goroutine to receive csv record line by line
		recordCSVPath := path.Join(recordDir, fileprefix+".csv")

		data := filehelper.ReadCsv(target)

		go func(ctx context.Context, csvPath string, csvChan chan string) {
			f, err := os.OpenFile(csvPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
			if err != nil {
				// if we failed to open file handle then quit import should be a better choice
				panic(err)
			}
			defer f.Close()
			for {
				select {
				case <-shutdownChan:
					saveRecords(records, recordPath)
					saveDownloader(downloaderRecord, recordErrorPath)
					os.Exit(1)
				case <-fileDownloaderErrorChan:
					saveRecords(records, recordPath)
					saveDownloader(downloaderRecord, recordErrorPath)
				case <-ctx.Done():
					fmt.Println("=====ctx.Done===")
					return
				case d := <-fileDownloaderChan:
					if d.Done {
						delete(downloaderRecord, d.Url)
					} else {
						downloaderRecord[d.Url] = &d
					}

				case record := <-csvChan:
					if record == "" { // chan closed
						err = saveRecords(records, recordPath)
						if err != nil {
							ferr = err
						}
						return
					}
					if _, err := f.WriteString(record); err != nil {
						log.Error(err)
					}
				}
			}

		}(ctx, recordCSVPath, csvChan)

		for _, v := range data {
			wg.Add(1)

			go func(v []string) {
				defer func() {
					<-pchan
					wg.Done()
				}()
				pchan <- struct{}{}

				url := fmt.Sprintf("https://%v.s3.%v.amazonaws.com/%v", v[0], v[1], v[2])

				// ignore file which has been imported
				lock.RLock()
				if _, ok := records[v[2]]; ok {
					lock.RUnlock()
					return
				}
				lock.RUnlock()

				var err error
				d := NewFileDownloader(url, downloaderRecord)

				fileTotalSize, err := d.getHeaderInfo()
				if err != nil {
					fmt.Printf("==get header info error:==%+v", err)
					return
				}

				d.FileSize = fileTotalSize

				for {
					if d.FileSize < d.RangStart {
						fileDownloaderChan <- FileDownloader{Url: d.Url, Done: true}
						break
					}
					err = d.runDownload(ctx, dagServ, cidBuilder, batchReadNum)
					if err != nil {
						fileDownloaderErrorChan <- struct{}{}
						return
					}
					d.Done = false
					fileDownloaderChan <- *d
				}

				cid, err := importer.BuildCidByLinks(ctx, d.DataLinks, dagServ)
				if err != nil {
					ctx.Done()
					return
				}

				lock.Lock()
				defer lock.Unlock()
				if i, err := strconv.Atoi(v[3]); err == nil {
					atomic.AddUint64(&importedSize, uint64(i))
					records[v[2]] = &MetaData{
						Name: v[0],
						Path: v[2],
						Size: int64(i),
						CID:  cid.String(),
					}
				}

				importedFiles += 1

				if totalSize > 0 {
					fmt.Printf("total %d files, imported %d files, %.2f %%\n", totalFiles, importedFiles, float64(importedFiles)/float64(totalFiles)*100)
					fmt.Printf("total size: %d, imported size: %d, %.2f %%\n", totalSize, importedSize, float64(importedSize)/float64(totalSize)*100)
				}
				if i, err := strconv.Atoi(v[3]); err == nil {
					csvChan <- fmt.Sprintf("%s,%s,%d\n", v[2], cid.String(), i)
				}

				if len(data) == len(records) {
					saveRecords(records, recordPath)
				}
			}(v)
		}

	}
	wg.Wait()

	return ferr
}

func (d *FileDownloader) getHeaderInfo() (int, error) {
	headers := map[string]string{}
	r, err := getNewRequest(d.Url, "HEAD", headers)
	if err != nil {
		return 0, err
	}

	client := retryablehttp.NewClient()
	client.Logger = nil
	resp, err := client.Do(r)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode > 300 || resp.StatusCode < 200 {
		return 0, fmt.Errorf("can't process, response is %v", resp.StatusCode)
	}

	if resp.Header.Get("Accept-Ranges") != "bytes" {
		return 0, errors.New("服务器不支持文件断点续传")
	}

	outputFileName, err := parseFileInfo(resp)
	if err != nil {
		return 0, fmt.Errorf("get file info err: %v", err)
	}
	if d.OutputFileName == "" {
		d.OutputFileName = outputFileName
	}

	return strconv.Atoi(resp.Header.Get("Content-Length"))
}

func parseFileInfo(resp *http.Response) (string, error) {
	contentDisposition := resp.Header.Get("Content-Disposition")
	if contentDisposition != "" {
		_, params, err := mime.ParseMediaType(contentDisposition)
		if err != nil {
			return "", err
		}
		return params["filename"], nil
	}

	filename := filepath.Base(resp.Request.URL.Path)
	return filename, nil
}

func (d *FileDownloader) runDownload(ctx context.Context, dagServ format.DAGService, cidBuilder cid.Builder, batchReadNum int) error {

	_filepath := fmt.Sprintf("download/%d_%s", d.RangIndex, d.OutputFileName)

	fileDir := filepath.Dir(_filepath)

	err := os.MkdirAll(fileDir, 0666)
	if err != nil {
		return err
	}

	headers := map[string]string{}
	size := importer.UnixfsChunkSize * uint64(batchReadNum)
	rangEnd := d.RangStart + (int(size) - 1)
	if err == nil {
		headers["Range"] = fmt.Sprintf("bytes=%v-%v", d.RangStart, rangEnd)
	}

	r, err := getNewRequest(d.Url, "GET", headers)
	if err != nil {
		return err
	}

	client := retryablehttp.NewClient()
	client.RetryMax = 1
	client.Logger = nil
	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode > 300 || resp.StatusCode < 200 {
		return fmt.Errorf("status code: %v", resp.StatusCode)
	}
	defer resp.Body.Close()

	dataLinks, err := importer.BalanceNodeV1(ctx, resp.Body, resp.ContentLength, dagServ, cidBuilder, batchReadNum)
	if err != nil {
		return err
	}

	d.DataLinks = append(d.DataLinks, dataLinks...)
	d.RangStart = rangEnd + 1
	d.RangIndex += 1
	return nil
}

func NewFileDownloader(url string, fileDownloadMap map[string]*FileDownloader) *FileDownloader {
	d := fileDownloadMap[url]
	if d != nil {
		return d
	}
	return &FileDownloader{
		FileSize: 0,
		Url:      url,
	}
}

type FileDownloader struct {
	DataLinks      []*importer.LinkAndSize
	RangIndex      int
	RangStart      int
	FileSize       int
	Url            string
	OutputFileName string
	Done           bool
}

func getNewRequest(url, method string, headers map[string]string) (*retryablehttp.Request, error) {
	r, err := retryablehttp.NewRequest(method,
		url,
		nil,
	)

	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}

	return r, nil
}

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

func saveDownloader(data interface{}, path string) error {
	bs, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return os.WriteFile(path, bs, 0666)
}

func readDownloader(path string) (map[string]*FileDownloader, error) {
	res := make(map[string]*FileDownloader)
	bs, err := os.ReadFile(path)
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
