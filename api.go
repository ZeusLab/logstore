package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

func responseOk(w http.ResponseWriter) {
	w.WriteHeader(200)
	_, err := fmt.Fprintln(w, "OK")
	if err != nil {
		log.Println("send response to agent get error", err)
	}
}

type InputLogPayload struct {
	Tag           string            `json:"tag,omitempty"`
	Timestamp     int64             `json:"timestamp,omitempty"`
	ContainerName string            `json:"container_name,omitempty"`
	Level         string            `json:"level,omitempty"`
	Message       string            `json:"message,omitempty"`
	Context       map[string]string `json:"context,omitempty"`
}

func mapKeys(m map[string]string) []string {
	if m == nil {
		return []string{}
	}
	rs := make([]string, 0, len(m))
	for k, _ := range m {
		rs = append(rs, k)
	}
	return rs
}

func mapValues(m map[string]string) []string {
	if m == nil {
		return []string{}
	}
	rs := make([]string, 0, len(m))
	for _, v := range m {
		rs = append(rs, v)
	}
	return rs
}

// /api/logs
func collectLog(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	tagValues := r.URL.Query()["tag"]
	defer func(w http.ResponseWriter) {
		responseOk(w)
	}(w)
	if tagValues == nil || len(tagValues) == 0 {
		return
	}
	tag := strings.TrimSpace(tagValues[0])
	defer func() {
		_ = r.Body.Close()
	}()
	data, err := ioutil.ReadAll(r.Body)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	logEntries := make([]LogEntry, 0)

	for scanner.Scan() {
		text := scanner.Text()
		if text != "" {
			if debug {
				log.Println(text)
			}
			var inputLog InputLogPayload
			err = json.Unmarshal([]byte(text), &inputLog)
			if err != nil {
				log.Println("can not unmarshal log from json", err)
				continue
			}
			if strIsEmpty(inputLog.Message) {
				continue
			}
			logEntries = append(logEntries, LogEntry{
				Id:            int64(snowFlake.Generate()),
				Tag:           tag,
				Timestamp:     inputLog.Timestamp,
				Date:          toYYYYMMDD(inputLog.Timestamp),
				ContainerName: inputLog.ContainerName,
				ContextKeys:   mapKeys(inputLog.Context),
				ContextValues: mapValues(inputLog.Context),
			})
		}
	}

	c, err := dbPool.acquire()
	if err != nil {
		log.Println("can not acquire connection to database", err)
		return
	}

	if c == nil {
		log.Println("connection is nil")
		return
	}
	defer func() {
		_ = dbPool.release(c)
	}()
	err = c.insert(logEntries)
	if err != nil {
		log.Println("insert log into log table in click-house get error", err)
	}
}

type OutputMessage struct {
	Code    int32  `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type OutputTagMessage struct {
	OutputMessage
	Data []string `json:"data,omitempty"`
}

type OutputTagHistoryMessage struct {
	OutputMessage
	Data []OutputTagHistoryPayload `json:"data,omitempty"`
}

type OutputTagHistoryPayload struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type OutputLogMessage struct {
	OutputMessage
	Data []OutputLogPayload `json:"data,omitempty"`
}

type OutputLogPayload struct {
	Id    int64  `json:"id,omitempty"`
	IdStr string `json:"id_str,omitempty"`
	Date  string `json:"date,omitempty"`
	InputLogPayload
}

func marshalJson(v interface{}) ([]byte, error) {
	bytes, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func writeHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Server", fmt.Sprintf("hermes %s", version))
	w.WriteHeader(200)
}

func writeResponse(w http.ResponseWriter, i interface{}) {
	bytes, err := marshalJson(i)
	if err != nil {
		bytes, _ = marshalJson(OutputMessage{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		})
		_, _ = w.Write(bytes)
		return
	}
	_, _ = w.Write(bytes)
}

func retrieveListOfTag(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	writeHeader(w)
	response := OutputTagMessage{
		OutputMessage: OutputMessage{
			Code:    http.StatusOK,
			Message: "OK",
		},
	}
	defer writeResponse(w, response)

	c, err := dbPool.acquire()
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}

	if c == nil {
		response.Code = http.StatusInternalServerError
		response.Message = "can not acquire db connection"
		return
	}

	defer func() {
		_ = dbPool.release(c)
	}()
	list, err := c.getAllTags()
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}
	response.Data = list
}

func retrieveListOfTagHistory(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	writeHeader(w)
	response := OutputTagHistoryMessage{
		OutputMessage: OutputMessage{
			Code:    http.StatusOK,
			Message: "OK",
		},
	}
	defer writeResponse(w, response)

	c, err := dbPool.acquire()
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}

	if c == nil {
		response.Code = http.StatusInternalServerError
		response.Message = "can not acquire db connection"
		return
	}

	defer func() {
		_ = dbPool.release(c)
	}()

	tags := r.URL.Query()["tag"]
	if tags == nil || len(tags) <= 0 {
		response.Code = http.StatusBadRequest
		response.Message = "missing tag"
		return
	}

	list, err := c.getHistoryOfTag(tags[0])
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}

	rs := make([]OutputTagHistoryPayload, 0)
	for _, v := range list {
		r := fmt.Sprintf("%s-%02s-%02s", v[0:4], v[4:6], v[6:8])
		rs = append(rs, OutputTagHistoryPayload{
			Key:   v,
			Value: r,
		})
	}

	response.Data = rs
}

func retrieveLog(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	writeHeader(w)
	response := OutputLogMessage{
		OutputMessage: OutputMessage{
			Code:    200,
			Message: "OK",
		},
	}
	defer writeResponse(w, response)

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

	c, err := dbPool.acquire()
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}

	if c == nil {
		response.Code = http.StatusInternalServerError
		response.Message = "can not acquire db connection"
		return
	}

	defer func() {
		_ = dbPool.release(c)
	}()

	logEntries, err := c.getLog()
	if err != nil {
		response.Code = http.StatusInternalServerError
		response.Message = err.Error()
		return
	}
	for i := len(logEntries)/2 - 1; i >= 0; i-- {
		opp := len(logEntries) - 1 - i
		logEntries[i], logEntries[opp] = logEntries[opp], logEntries[i]
	}
	response.Data = nil
}
