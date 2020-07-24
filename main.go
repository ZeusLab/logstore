package main

import (
	"flag"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"os"
)

var version = "1.0.0"

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
