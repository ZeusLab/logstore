package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type LogMessage struct {
	Id            int64   `json:"id"`
	Tag           string  `json:"fluentd_tag"`
	Date          string  `json:"date"`
	Timestamp     int64   `json:"fluentd_time"`
	ContainerId   string  `json:"container_id"`
	ContainerName string  `json:"container_name"`
	Message       *string `json:"message"`
	Log           *string `json:"log"`
}

type Logs struct {
	Code    int          `json:"code"`
	Message string       `json:"message"`
	Data    []LogMessage `json:"data"`
}

func isStringNilOrEmpty(s *string) bool {
	return s == nil || strings.TrimSpace(*s) == ""
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
			if debug {
				log.Println(text)
			}
			var lm LogMessage
			err = json.Unmarshal([]byte(text), &lm)
			if err != nil {
				log.Println("can not unmarshal log from json", err)
				continue
			}
			if isStringNilOrEmpty(lm.Log) && isStringNilOrEmpty(lm.Message) {
				continue
			}
			if isStringNilOrEmpty(lm.Log) {
				lm.Log = lm.Message
			}
			lm.Message = lm.Log
			for _, tag := range tags {
				lm.Tag = tag
				lm.Id = int64(snowFlake.Generate())
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

func MarshallJson(v interface{}) ([]byte, error) {
	bytes, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func retrieveLog(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	tagValues := r.URL.Query()["tag"]
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Server", "zeus-mfe-master")
	w.WriteHeader(200)
	logs := Logs{
		Code:    200,
		Message: "OK",
	}
	defer func(l *Logs) {
		response, err := MarshallJson(l)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(response)
	}(&logs)
	if tagValues == nil || len(tagValues) == 0 {
		logs.Code = http.StatusBadRequest
		logs.Message = "missing tag"
		return
	}

	dateValues := r.URL.Query()["date"]
	date := time.Now().Format("20060102")
	if dateValues != nil && len(dateValues) > 0 {
		date = dateValues[0]
	}
	lms, err := selectLog(tagValues[0], date, 100)
	if err != nil {
		logs.Code = http.StatusInternalServerError
		logs.Message = err.Error()
		return
	}
	logs.Data = lms
}

func main() {
	log.SetOutput(os.Stdout)

	flag.StringVar(&clickHouseAddress, "ch-address", "clickhouse:9000", "address of click-house database")
	flag.BoolVar(&debug, "debug", false, "in debug mode, more message will be displayed")
	flag.Int64Var(&nodeId, "node-id", 1, "node id")
	flag.Parse()

	var err error
	snowFlake, err = NewNode(nodeId)
	if err != nil {
		log.Fatal(err)
	}

	router := httprouter.New()
	router.POST("/log", collectLog)
	router.GET("/log", retrieveLog)
	log.Fatal(http.ListenAndServe(":80", router))
}
