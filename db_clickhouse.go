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

func openConnection() (*sql.DB, error) {
	connect, err := sql.Open("clickhouse", getClickHouseUrl())
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not connect to click-house db %v", err))
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return nil, errors.New(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		}
		return nil, err
	}
	return connect, nil
}

func toYYYYMMDD(timestamp int64) string {
	return time.Unix(0, timestamp*int64(time.Millisecond)).Format("20060102")
}

func insert(lms []LogMessage) error {
	connect, err := openConnection()
	if err != nil {
		return errors.New(fmt.Sprintf("can not open connection to click-house db %v", err))
	}
	tx, err := connect.Begin()
	if err != nil {
		return errors.New(fmt.Sprintf("open click-house tx get error %v", err))
	}
	stmt, _ := tx.Prepare(`INSERT INTO hermes.logs(id, application, timestamp, date, container_name, container_id, message) VALUES (?,?,?,?,?,?)`)
	defer func() {
		_ = stmt.Close()
	}()
	for _, lm := range lms {
		_, err := stmt.Exec(
			lm.Id,
			lm.Tag,
			lm.Timestamp,
			toYYYYMMDD(lm.Timestamp),
			lm.ContainerName,
			lm.ContainerId,
			*lm.Message,
		)
		if err != nil {
			log.Println(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.New(fmt.Sprintf("commit log to click-house db get error %v", err))
	}
	return nil
}

func selectLog(application, date string, limit int) ([]LogMessage, error) {
	connect, err := openConnection()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not open connection to click-house db %v", err))
	}
	defer func() {
		_ = connect.Close()
	}()
	rows, err := connect.Query(`SELECT id, container_id, container_name, timestamp, message FROM hermes.logs
		WHERE (application = ? AND date = ?)
		ORDER BY timestamp DESC
		LIMIT ?
	`, application, date, limit)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	lms := make([]LogMessage, 0)
	for rows.Next() {
		var (
			id            int64
			containerId   string
			containerName string
			timestamp     int64
			message       string
		)
		if err := rows.Scan(&id, &containerId, &containerName, &timestamp, &message); err != nil {
			return nil, err
		}
		lms = append(lms, LogMessage{
			Id:            id,
			Tag:           application,
			Date:          date,
			ContainerId:   containerId,
			ContainerName: containerName,
			Timestamp:     timestamp,
			Message:       &message,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return lms, nil
}
