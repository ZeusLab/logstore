package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	. "hermes/core"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

func collectLog(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	/** immediately response to client */
	w.WriteHeader(http.StatusOK)
	_, err := fmt.Fprintln(w, "OK")
	if err != nil {
		log.Printf("send response to agent get error %v\n", err)
	}
	/** end response */

	tagValues := r.URL.Query()["tag"]
	if tagValues == nil || len(tagValues) == 0 {
		return
	}
	tag := strings.TrimSpace(tagValues[0])
	defer func() {
		_ = r.Body.Close()
	}()
	data, err := ioutil.ReadAll(r.Body)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))

	inputs := make([]InputLogPayload, 0)
	for scanner.Scan() {
		text := scanner.Text()
		if text != "" {
			var inputLog InputLogPayload
			err = json.Unmarshal([]byte(text), &inputLog)
			if err != nil {
				log.Println("can not unmarshal log from json", err)
				continue
			}
			if StrIsEmpty(inputLog.Message) {
				continue
			}
			inputLog.Tag = tag
			inputs = append(inputs, inputLog)
		}
	}

	for name, driver := range drivers {
		err = driver.Collect(inputs)
		if err != nil {
			log.Printf("collect log in driver %s get error %v \n", name, err)
		}
	}
}
