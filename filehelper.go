package filehelper

import (
	"encoding/csv"
	"fmt"
	mrand "math/rand"
	"os"
	"time"
)

const (
	RandPortMin = 4000
	RandPortMax = 65000
)

func RandPort() string {
	mrand.Seed(time.Now().Unix() * int64(mrand.Intn(RandPortMax)))
	r := mrand.Float64()
	m := RandPortMin + (RandPortMax-RandPortMin)*r
	return fmt.Sprintf("%.0f", m)
}

func ExistDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func ReadCsv(filepath string) [][]string {
	opencast, err := os.Open(filepath)
	if err != nil {
		return [][]string{}
	}
	defer opencast.Close()

	ReadCsv := csv.NewReader(opencast)
	readAll, err := ReadCsv.ReadAll()
	if err != nil {
		panic("open csv file failed")
	}
	return readAll[1:]
}

func ReadCsvNoTitle(filepath string) [][]string {
	opencast, err := os.Open(filepath)
	if err != nil {
		return [][]string{}
	}
	defer opencast.Close()

	ReadCsv := csv.NewReader(opencast)
	readAll, err := ReadCsv.ReadAll()
	if err != nil {
		panic("open csv file failed")
	}
	return readAll
}
