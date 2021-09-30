package filehelper

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func FileWalkAsync(args []string) chan Finfo {
	fichan := make(chan Finfo)
	go func() {
		defer close(fichan)
		for _, path := range args {
			finfo, err := os.Stat(path)
			if err != nil {
				return
			}
			// 忽略隐藏目录
			if strings.HasPrefix(finfo.Name(), ".") {
				continue
			}
			if finfo.IsDir() {
				files, err := ioutil.ReadDir(path)
				if err != nil {
					return
				}
				templist := make([]string, 0)
				for _, n := range files {
					templist = append(templist, fmt.Sprintf("%s/%s", path, n.Name()))
				}
				embededChan := FileWalkAsync(templist)
				if err != nil {
					return
				}

				for item := range embededChan {
					fichan <- item
				}
			} else {
				fichan <- Finfo{
					Path: path,
					Name: finfo.Name(),
					Info: finfo,
				}
			}
		}
	}()

	return fichan
}

func FileWalkSync(args []string) (fileList []string, err error) {
	fileList = make([]string, 0)
	for _, path := range args {
		finfo, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		// 忽略隐藏目录
		if strings.HasPrefix(finfo.Name(), ".") {
			continue
		}
		if finfo.IsDir() {
			files, err := ioutil.ReadDir(path)
			if err != nil {
				return nil, err
			}
			templist := make([]string, 0)
			for _, n := range files {
				templist = append(templist, fmt.Sprintf("%s/%s", path, n.Name()))
			}
			list, err := FileWalkSync(templist)
			if err != nil {
				return nil, err
			}
			fileList = append(fileList, list...)
		} else {
			fileList = append(fileList, path)
		}
	}

	return
}
