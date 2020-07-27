package main

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"strings"
)

const (
	CmdRequestVote = iota
	CmdAppendEntry
)

type RequestVoteMessage struct {
}

type AppendEntry struct {
}

func clusterCommand(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	c := r.URL.Query()["c"]
	if c == nil || len(c) == 0 {
		return
	}
	cmd := strings.TrimSpace(c[0])
	log.Printf("receive command %s from node %s\n", cmd, r.RemoteAddr)
	switch cmd {
	case "requestVote":
		requestVote(w, r, nil)
		break
	case "appendEntry":
		appendEntry(w, r, nil)
		break
	default:
		w.WriteHeader(http.StatusBadRequest)
		_, err := fmt.Fprintln(w, "Command is not correct")
		if err != nil {
			log.Printf("send response to agent get error %v\n", err)
		}
	}
}

func requestVote(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

}

func appendEntry(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

}
