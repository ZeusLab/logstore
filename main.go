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
)

type LogMessage struct {
	Id            int64   `json:"id"`
	Tag           string  `json:"fluentd_tag"`
	Date          string  `json:"date"`
	Timestamp     int64   `json:"fluentd_time"`
	ContainerId   string  `json:"container_id"`
	ContainerName string  `json:"container_name"`
	Message       *string `json:"message,omitempty"`
	Log           *string `json:"log,omitempty"`
}

type CommonResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type Logs struct {
	CommonResponse
	Data []LogMessage `json:"data"`
}

type Applications struct {
	CommonResponse
	Data []string `json:"data"`
}

type Histories struct {
	CommonResponse
	Data []HistoryItem `json:"data"`
}

type HistoryItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func isStringNilOrEmpty(s *string) bool {
	return s == nil || strings.TrimSpace(*s) == ""
}

func collectLog(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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

func retriveHistory(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Server", "zeus-mfe-master")
	w.WriteHeader(200)
	response := Histories{
		CommonResponse: CommonResponse{
			Code:    200,
			Message: "OK",
		},
	}
	defer func(l *Histories) {
		bytes, err := MarshallJson(l)
		if err != nil {
			response.Code = http.StatusInternalServerError
			response.Message = err.Error()
		}
		_, _ = w.Write(bytes)
	}(&response)

	tags := r.URL.Query()["tag"]
	if tags == nil || len(tags) <= 0 {
		response.Code = http.StatusBadRequest
		response.Message = "missing tag"
		return
	}

	list, err := selectAppHistories(tags[0])
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}

	rs := make([]HistoryItem, 0)
	for _, v := range list {
		r := fmt.Sprintf("%s-%02s-%02s", v[0:4], v[5:6], v[7:8])
		rs = append(rs, HistoryItem{
			Key:   v,
			Value: r,
		})
	}

	response.Data = rs
}

func retriveListApplication(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Server", "zeus-mfe-master")
	w.WriteHeader(200)
	response := Applications{
		CommonResponse: CommonResponse{
			Code:    200,
			Message: "OK",
		},
	}
	defer func(l *Applications) {
		bytes, err := MarshallJson(l)
		if err != nil {
			response.Code = http.StatusInternalServerError
			response.Message = err.Error()
		}
		_, _ = w.Write(bytes)
	}(&response)

	list, err := selectDistinctApplication()
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}
	response.Data = list
}

func retrieveLog(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Server", "zeus-mfe-master")
	w.WriteHeader(200)
	response := Logs{
		CommonResponse: CommonResponse{
			Code:    200,
			Message: "OK",
		},
	}
	defer func(l *Logs) {
		bytes, err := MarshallJson(l)
		if err != nil {
			response.Code = http.StatusInternalServerError
			response.Message = err.Error()
		}
		_, _ = w.Write(bytes)
	}(&response)

	opts, err := createOption(r.URL.Query())
	if err != nil {
		if err == errorMissingTag {
			response.Code = http.StatusBadRequest
			response.Message = "missing tag"
			return
		}

		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}

	lms, err := selectLogWithOpt(*opts)
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}
	response.Data = lms
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
	router.GET("/application", retriveListApplication)
	router.GET("/histories", retriveHistory)
	log.Fatal(http.ListenAndServe(":80", router))
}
