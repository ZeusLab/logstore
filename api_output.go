package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	. "hermes/core"
	"log"
	"net/http"
)

func retrieveListOfTag(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Server", fmt.Sprintf("hermes %s", version))
	writeResponse := func(w http.ResponseWriter, i interface{}) {
		bytes, err := json.Marshal(i)
		if err != nil {
			log.Println("marshal response get error", err)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprintln(w, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(bytes)
	}

	response := OutputTagMessage{
		OutputMessage: OutputMessage{
			Code:    http.StatusOK,
			Message: "OK",
		},
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer func() {
		cancel()
	}()
	list, err := mainStorage.FindAllTag(ctx)
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
	} else {
		response.Data = list
	}
	writeResponse(w, response)
}
