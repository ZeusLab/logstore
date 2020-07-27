package main

import (
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"path"
	"path/filepath"
	"strings"
)

func webInterface(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Println(r.RemoteAddr, " ", r.Method, " ", r.URL)
	upath := r.URL.Path
	if !strings.HasPrefix(upath, "/") {
		upath = "/" + upath
	}
	upath = strings.TrimPrefix(upath, "/web")
	r.URL.Path = upath
	http.ServeFile(w, r, filepath.Join(webRoot, path.Clean(upath)))
}
