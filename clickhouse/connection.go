package clickhouse

import (
	"database/sql"
	"errors"
	"fmt"
	. "hermes/core"
	"log"
	"net/http"
)

type Connection struct {
	closed bool
	conn   *sql.DB
	ts     int64
}

func (c *Connection) Close() error {
	c.closed = true
	c.ts = 0
	return c.conn.Close()
}

func (c *Connection) GetAllTags() ([]string, error) {
	selectScript := fmt.Sprintf(`SELECT DISTINCT(tag) FROM %s.%s`, DatabaseName, LogTableName)
	log.Println(`query:`, selectScript)
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

func (c *Connection) Insert(logs []LogEntry) error {
	tx, err := c.conn.Begin()
	if err != nil {
		return errors.New(fmt.Sprintf("open click-house tx get error %v", err))
	}
	insertScript := fmt.Sprintf(`INSERT INTO %s.%s(id, tag, timestamp, date, container_name, level, message, context.keys, context.values) VALUES (?,?,?,?,?,?,?,?,?)`, DatabaseName, LogTableName)
	stmt, _ := tx.Prepare(insertScript)
	defer func() {
		_ = stmt.Close()
	}()
	for _, logEntry := range logs {
		_, err := stmt.Exec(
			NextId(),
			logEntry.Tag,
			logEntry.Timestamp,
			ToYYYYMMDD(logEntry.Timestamp),
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
