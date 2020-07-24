package core

import (
	"strings"
	"time"
)

func StrIsEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

func ToYYYYMMDD(timestamp int64) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format("20060102")
}
