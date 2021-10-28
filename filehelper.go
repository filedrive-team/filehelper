package filehelper

import (
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
