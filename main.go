package main

import (
	"flag"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"os"
)

var version = "1.0.0"

//type LogMessage struct {
//	IdStr         string  `json:"id_str"`
//	Id            int64   `json:"id"`
//	Tag           string  `json:"fluentd_tag"`
//	Date          string  `json:"date"`
//	Timestamp     int64   `json:"fluentd_time"`
//	ContainerId   string  `json:"container_id"`
//	ContainerName string  `json:"container_name"`
//	Message       *string `json:"message,omitempty"`
//	Log           *string `json:"log,omitempty"`
//}
//
//type CommonResponse struct {
//	Code    int    `json:"code"`
//	Message string `json:"message"`
//}
//
//type Logs struct {
//	CommonResponse
//	Data []LogMessage `json:"data"`
//}
//
//type Applications struct {
//	CommonResponse
//	Data []string `json:"data"`
//}
//
//type Histories struct {
//	CommonResponse
//	Data []HistoryItem `json:"data"`
//}
//
//type HistoryItem struct {
//	Key   string `json:"key"`
//	Value string `json:"value"`
//}
//
//func isStringNilOrEmpty(s *string) bool {
//	return s == nil || strings.TrimSpace(*s) == ""
//}
//
//func responseOk(w http.ResponseWriter) {
//	w.WriteHeader(200)
//	_, err := fmt.Fprintln(w, "OK")
//	if err != nil {
//		log.Println("send response to agent get error", err)
//	}
//}
//func MarshallJson(v interface{}) ([]byte, error) {
//	bytes, err := json.Marshal(v)
//	if err != nil {
//		return nil, err
//	}
//	return bytes, nil
//}

//func retrieveHistory(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
//	w.Header().Set("Content-Type", "application/json")
//	w.Header().Set("Server", "zeus-mfe-master")
//	w.WriteHeader(200)
//	response := Histories{
//		CommonResponse: CommonResponse{
//			Code:    200,
//			Message: "OK",
//		},
//	}
//	defer func(l *Histories) {
//		bytes, err := MarshallJson(l)
//		if err != nil {
//			response.Code = http.StatusInternalServerError
//			response.Message = err.Error()
//		}
//		_, _ = w.Write(bytes)
//	}(&response)
//
//	tags := r.URL.Query()["tag"]
//	if tags == nil || len(tags) <= 0 {
//		response.Code = http.StatusBadRequest
//		response.Message = "missing tag"
//		return
//	}
//
//	list, err := selectAppHistories(tags[0])
//	if err != nil {
//		response.Code = http.StatusInternalServerError
//		response.Message = err.Error()
//		return
//	}
//
//	rs := make([]HistoryItem, 0)
//	for _, v := range list {
//		r := fmt.Sprintf("%s-%02s-%02s", v[0:4], v[4:6], v[6:8])
//		rs = append(rs, HistoryItem{
//			Key:   v,
//			Value: r,
//		})
//	}
//
//	response.Data = rs
//}

//func retrieveListApplication(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
//	w.Header().Set("Content-Type", "application/json")
//	w.Header().Set("Server", "zeus-mfe-master")
//	w.WriteHeader(200)
//	response := Applications{
//		CommonResponse: CommonResponse{
//			Code:    200,
//			Message: "OK",
//		},
//	}
//	defer func(l *Applications) {
//		bytes, err := MarshallJson(l)
//		if err != nil {
//			response.Code = http.StatusInternalServerError
//			response.Message = err.Error()
//		}
//		_, _ = w.Write(bytes)
//	}(&response)
//
//	list, err := selectDistinctApplication()
//	if err != nil {
//		response.Code = http.StatusInternalServerError
//		response.Message = err.Error()
//		return
//	}
//	response.Data = list
//}

//func retrieveLog(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
//	w.Header().Set("Content-Type", "application/json")
//	w.Header().Set("Server", "zeus-mfe-master")
//	w.WriteHeader(200)
//	response := Logs{
//		CommonResponse: CommonResponse{
//			Code:    200,
//			Message: "OK",
//		},
//	}
//	defer func(l *Logs) {
//		bytes, err := MarshallJson(l)
//		if err != nil {
//			response.Code = http.StatusInternalServerError
//			response.Message = err.Error()
//		}
//		_, _ = w.Write(bytes)
//	}(&response)
//
//	opts, err := createOption(r.URL.Query())
//	if err != nil {
//		if err == errorMissingTag {
//			response.Code = http.StatusBadRequest
//			response.Message = "missing tag"
//			return
//		}
//
//		response.Code = http.StatusInternalServerError
//		response.Message = err.Error()
//		return
//	}
//
//	lms, err := selectLogWithOpt(*opts)
//	if err != nil {
//		response.Code = http.StatusInternalServerError
//		response.Message = err.Error()
//		return
//	}
//	for i := len(lms)/2 - 1; i >= 0; i-- {
//		opp := len(lms) - 1 - i
//		lms[i], lms[opp] = lms[opp], lms[i]
//	}
//	response.Data = lms
//}

func main() {
	log.SetOutput(os.Stdout)
	flag.StringVar(&clickHouseAddress, "ch-address", "clickhouse:9000", "address of click-house database")
	flag.IntVar(&minActiveConnection, "ch-conn-min", 0, "minimum number of active connection")
	flag.IntVar(&maxActiveConnection, "ch-conn-max", 1, "maximum number of active connection")
	flag.Int64Var(&maxConnectionLifeTime, "ch-conn-ttl", 30000, "maximum inactive duration of connection")
	flag.BoolVar(&debug, "debug", false, "in debug mode, more message will be displayed")
	flag.Int64Var(&nodeId, "node-id", 1, "node id")
	flag.Parse()

	var err error
	snowFlake, err = NewNode(nodeId)
	if err != nil {
		log.Fatal(err)
	}

	dbPool, err = CreateCHPool(0, 10, 60000, "")
	if err != nil {
		log.Fatal(err)
	}

	router := httprouter.New()
	router.POST("/api/logs", collectLog)
	router.GET("/api/logs", retrieveLog)
	router.GET("/api/applications", retrieveListOfTag)
	router.GET("/api/histories", retrieveListOfTagHistory)
	log.Fatal(http.ListenAndServe(":80", router))
}
