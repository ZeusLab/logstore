package main

import (
	"strings"
	"time"
)

func strIsEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

func toYYYYMMDD(timestamp int64) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format("20060102")
}
