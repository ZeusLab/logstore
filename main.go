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
	flag.StringVar(&webRoot, "web-root", "", "")
	flag.StringVar(&certPem, "ssl-cert", "/certs/cert.pem", "")
	flag.StringVar(&keyPerm, "ssl-key", "/certs/key.pem", "")
	flag.BoolVar(&sslEnabled, "ssl", false, "")
	flag.Parse()

	var err error

	directory, err := filepath.Abs(".")
	if err != nil {
		log.Fatal(err)
	}

	if StrIsEmpty(webRoot) {
		webRoot = directory
	}

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
	router.GlobalOPTIONS = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Access-Control-Request-Method") != "" {
			// Set CORS headers
			header := w.Header()
			header.Set("Access-Control-Allow-Methods", r.Header.Get("Allow"))
			header.Set("Access-Control-Allow-Origin", "*")
			header.Set("Access-Control-Allow-Headers", "*")
			header.Set("Access-Control-Allow-Credentials", "true")
			header.Set("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate")
			header.Set("Pragma", "no-cache")
			header.Set("Expires", "0")
		}

		// Adjust status code to 204
		w.WriteHeader(http.StatusNoContent)
	})

	router.POST("/cluster", clusterCommand)
	router.POST("/api/log", collectLog)
	router.GET("/api/tag", retrieveListOfTag)
	router.GET("/ws", webSocket)
	router.GET("/web", webInterface)

	p := config.Port
	if port > 0 {
		p = port
	}

	if p <= 0 {
		log.Fatal("port number must not be negative")
	}

	if !sslEnabled {
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", p), router))
		return
	}

	err = DoesFileExist(certPem)
	if err != nil {
		log.Fatal(err)
		return
	}

	err = DoesFileExist(keyPerm)
	if err != nil {
		log.Fatal(err)
		return
	}
	if port == 80 {
		port = 443
	}
	log.Fatal(http.ListenAndServeTLS(fmt.Sprintf(":%d", port), certPem, keyPerm, nil))
}
