package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"log"
	"time"
)

/**
┌─application─┬───timestamp───┬────date────┬────container name────┬────container id────┬─message─────┐
│ postgresql  │ 1588842127114 │ 20200507   │ /athena_postgres_1   │ 1f5552dba15dd0df   │ Hello world │
└─────────────┴───────────────┴────────────┴──────────────────────┘────────────────────┘─────────────┘
 */

func createTableIfNotExist() error {
	connect, err := sql.Open("clickhouse", "tcp://172.17.0.3:9000?debug=false")
	if err != nil {
		log.Fatal(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Println(err)
		}
		log.Println(err)
		return err
	}
	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS hermes.logs (
			application  	 String,
			timestamp 	     Int64,
			date         	 FixedString(8),
			container_name   String,
			container_id	 String,
			message  		String
		) ENGINE = MergeTree()
		PARTITION BY (application, date)
		ORDER BY timestamp
	`)
	if err != nil {
		return err
	}
	return nil
}

func toYYYYMMDD(timestamp int64) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format("20060102")
}

func insert(lms []LogMessage) error {
	connect, err := sql.Open("clickhouse", "tcp://172.17.0.3:9000?debug=false")
	if err != nil {
		return errors.New(fmt.Sprintf("can not connect to click-house db %v", err))
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return errors.New(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		}
		return err
	}

	tx, err := connect.Begin()
	if err != nil {
		return errors.New(fmt.Sprintf("open click-house tx get error %v", err))
	}
	stmt, _ := tx.Prepare(`INSERT INTO hermes.logs(application, timestamp, date, container_name, container_id, message) VALUES (?,?,?)`)
	defer func() {
		_ = stmt.Close()
	}()
	for _, lm := range lms {
		ts := int64(lm.Timestamp * 1000)
		_, err := stmt.Exec(
			lm.Tag,
			ts,
			toYYYYMMDD(ts),
			lm.ContainerName,
			lm.ContainerId,
			lm.Message)
		if err != nil {
			log.Println(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.New(fmt.Sprintf("commit log to click-house db get error %v", err))
	}
	return nil
}

func selectLog(){
	
}