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

func getClickHouseUrl() string {
	return fmt.Sprintf("tcp://%s?debug=%v", clickHouseAddress, debug)
}

func createTableIfNotExist() error {
	connect, err := sql.Open("clickhouse", getClickHouseUrl())
	if err != nil {
		log.Fatal(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return errors.New(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		}
		return err
	}
	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS hermes.logs (
			application      String,
			timestamp        Int64,
			date             FixedString(8),
			container_name   String,
			container_id     String,
			message          String
		) ENGINE = MergeTree()
		PARTITION BY (application, date)
		ORDER BY (application, timestamp)
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
	connect, err := sql.Open("clickhouse", getClickHouseUrl())
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
	stmt, _ := tx.Prepare(`INSERT INTO hermes.logs(application, timestamp, date, container_name, container_id, message) VALUES (?,?,?,?,?,?)`)
	defer func() {
		_ = stmt.Close()
	}()
	for _, lm := range lms {
		_, err := stmt.Exec(
			lm.Tag,
			lm.Timestamp,
			toYYYYMMDD(lm.Timestamp),
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

func selectLog(application, date string) {

}
