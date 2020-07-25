package main

import (
	"flag"
	"fmt"
	"github.com/julienschmidt/httprouter"
	. "hermes/core"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

var version = "1.0.0"
var config HermesConfig
var drivers = make(map[string]LogDriver)
var mainStorage LogDriver

func main() {
	log.SetOutput(os.Stdout)
	flag.IntVar(&port, "port", 0, "")
	flag.Int64Var(&nodeId, "node", 0, "")
	flag.StringVar(&configFile, "config", "", "")
	flag.Parse()

	var err error

	/** init id generator */
	err = InitIdGenerator(nodeId)
	if err != nil {
		log.Fatal(err)
	}

	/** init working directory and read configuration */
	workDir, err := filepath.Abs(".")
	if err != nil {
		log.Fatal(err)
	}

	if !StrIsEmpty(configFile) {
		workDir, _ = filepath.Split(configFile)
	} else {
		configFile = filepath.Join(workDir, "config.yaml")
	}

	config, err = ReadConfig(configFile)
	if err != nil {
		log.Fatal(err)
	}

	/** init drivers */
	for _, opt := range config.Drivers {
		driver, ok := drivers[opt.Name]
		if !ok || driver == nil {
			log.Fatal(fmt.Errorf("not found driver with name %s", opt.Name))
		}
		err = driver.Open(opt)
		if err != nil {
			log.Fatal(err)
		}
		if opt.IsMainStorage {
			if mainStorage != nil {
				log.Fatalln("found two driver are configured as main storage")
			}
			mainStorage = driver
		}
	}

	defer func() {
		for _, driver := range drivers {
			_ = driver.Close()
		}
	}()

	if mainStorage == nil {
		log.Fatalln("not found any driver is configured as main storage")
	}

	router := httprouter.New()
	router.POST("/api/log", collectLog)
	router.GET("/api/tag", retrieveListOfTag)
	router.GET("/ws", webSocket)

	p := config.Port
	if port > 0 {
		p = port
	}

	if p <= 0 {
		log.Fatal("port number must not be negative")
	}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", p), router))
}
