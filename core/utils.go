package core

import (
	"errors"
	"os"
	"strings"
	"time"
)

func StrIsEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

func ToYYYYMMDD(timestamp int64) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format("20060102")
}

func DoesFileExist(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	info, err := f.Stat()
	if err != nil {
		return err
	}

	if info.IsDir() {
		return errors.New(path + " is not a file")
	}
	return nil
}
