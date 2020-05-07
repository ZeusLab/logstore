package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

type LogMessage struct {
	Tag           string  `json:"fluentd_tag"`
	Timestamp     float64 `json:"fluentd_time"`
	Message       string  `json:"message"`
	ContainerId   string  `json:"container_id"`
	ContainerName string  `json:"container_name"`
	Log           string  `json:"log"`
}

func collectLog(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tagValues := r.URL.Query()["tag"]
	defer func(w http.ResponseWriter) {
		responseOk(w)
	}(w)
	if tagValues == nil || len(tagValues) == 0 {
		return
	}
	tags := strings.Split(tagValues[0], ",")
	defer func() {
		_ = r.Body.Close()
	}()
	data, err := ioutil.ReadAll(r.Body)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	lms := make([]LogMessage, 0)

	for scanner.Scan() {
		text := scanner.Text()
		if text != "" {
			var lm LogMessage
			err = json.Unmarshal([]byte(text), &lm)
			if err != nil {
				log.Println("can not unmarshal log from json", err)
				continue
			}
			if lm.Log == "" && lm.Message == "" {
				continue
			}
			if lm.Log == "" {
				lm.Log = lm.Message
			}
			lm.Message = lm.Log
			for _, tag := range tags {
				lm.Tag = tag
				lms = append(lms, lm)
			}
		}
	}
	err = insert(lms)
	if err != nil {
		log.Println("insert log into log table in click-house get error", err)
	}
}

func responseOk(w http.ResponseWriter) {
	w.WriteHeader(200)
	_, err := fmt.Fprintln(w, "OK")
	if err != nil {
		log.Println("send response to agent get error", err)
	}
}

func retrieveLog(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	responseOk(w)
}

func main() {
	log.SetOutput(os.Stdout)

	err := createTableIfNotExist()
	if err != nil {
		log.Fatal("create logs table in click-house get error", err)
	}

	router := httprouter.New()
	router.POST("/log", collectLog)
	router.GET("/log", retrieveLog)
	log.Fatal(http.ListenAndServe(":80", router))
}
