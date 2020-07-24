package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
)

var dbPool *CHPool

func (c *Connection) getAllTags() ([]string, error) {
	selectScript := fmt.Sprintf(`query select distinct(application) from %s`, LogTableName)
	log.Println(`query: `, selectScript)
	rows, err := c.conn.Query(selectScript)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	list := make([]string, 0)
	for rows.Next() {
		var appName string
		if err := rows.Scan(&appName); err != nil {
			return nil, err
		}
		list = append(list, appName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}

func (c *Connection) getHistoryOfTag(tag string) ([]string, error) {
	selectScript := fmt.Sprintf(`select distinct(date) from %s where tag = ? order by date desc`, LogTableName)
	log.Println(`query: `, selectScript)
	log.Println(`parameter: `, tag)
	rows, err := c.conn.Query(selectScript, tag)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = rows.Close()
	}()

	list := make([]string, 0)
	for rows.Next() {
		var appName string
		if err := rows.Scan(&appName); err != nil {
			return nil, err
		}
		list = append(list, appName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}

type SelectLogOption struct {
	Tag    string
	Limit  string
	LastId int64
	Date   string
}

func createSelectLogOption(r http.Request) (opt SelectLogOption, err error) {
	return
}

func (c *Connection) getLog(opt SelectLogOption) ([]LogEntry, error) {
	return nil, nil
}

func (c *Connection) insert(logs []LogEntry) error {
	tx, err := c.conn.Begin()
	if err != nil {
		return errors.New(fmt.Sprintf("open click-house tx get error %v", err))
	}
	insertScript := fmt.Sprintf(`INSERT INTO %s(id, tag, timestamp, date, container_name, level, message, context.keys, context.values) VALUES (?,?,?,?,?,?,?,?,?)`, LogTableName)
	stmt, _ := tx.Prepare(insertScript)
	defer func() {
		_ = stmt.Close()
	}()
	for _, logEntry := range logs {
		_, err := stmt.Exec(
			int64(snowFlake.Generate()),
			logEntry.Tag,
			logEntry.Timestamp,
			toYYYYMMDD(logEntry.Timestamp),
			logEntry.ContainerName,
			logEntry.Level,
			logEntry.Message,
			logEntry.ContextKeys,
			logEntry.ContextValues,
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
