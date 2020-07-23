package main

import "strings"

func strIsEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}
