package clickhouse

import (
	"context"
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

func (c *Connection) GetAllTags(ctx context.Context) ([]string, error) {
	selectScript := fmt.Sprintf(`SELECT DISTINCT(tag) FROM %s.%s`, DatabaseName, LogTableName)
	log.Println(`query:`, selectScript)
	rows, err := c.conn.QueryContext(ctx, selectScript)
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

//id, tag, timestamp, date, container_name, level, message, context.key, context.value
var logQueryScript = `SELECT id, tag, timestamp, date, container_name, level, message, context.key, context.value
 FROM %s.%s
 WHERE tag = ? AND level >= ? AND (timestamp >= ? AND timestamp <= ?)
 ORDER BY timestamp ASC
`

func (c *Connection) GetLog(ctx context.Context, opt QueryLogOption) error {
	selectScript := fmt.Sprintf(logQueryScript, DatabaseName, LogTableName)
	log.Println(`query:`, selectScript)
	rows, err := c.conn.QueryContext(ctx, selectScript, opt.Tag, opt.LogLevel, opt.StartTime, opt.EndTime)
	if err != nil {
		opt.Response <- OutputLogMessage{
			OutputMessage: OutputMessage{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			},
		}
		return err
	}

	defer func() {
		_ = rows.Close()
	}()

	list := make([]OutputLogPayload, opt.BatchSize)
	i := int32(0)
	totalMessage := 0
	for rows.Next() {
		totalMessage++
		var (
			id            int64
			tag           string
			timestamp     int64
			date          string
			containerName string
			level         int32
			message       string
			contextKeys   []string
			contextValues []string
		)
		if err := rows.Scan(&id, &tag,
			&timestamp, &date, &containerName,
			&level, &message,
			&contextKeys, &contextValues); err != nil {
			opt.Response <- OutputLogMessage{
				OutputMessage: OutputMessage{
					Code:    http.StatusInternalServerError,
					Message: err.Error(),
				},
			}
			return err
		}

		ctx := make(InputLogContext)
		if len(contextKeys) > 0 {
			for i, v := range contextKeys {
				ctx[v] = contextValues[i]
			}
		}

		list[i] = OutputLogPayload{
			Id:    id,
			IdStr: fmt.Sprintf("%d", id),
			Date:  date,
			InputLogPayload: InputLogPayload{
				Tag:           opt.Tag,
				Timestamp:     timestamp,
				ContainerName: containerName,
				Level:         LogLevelStr(level),
				Message:       message,
				Context:       ctx,
			},
		}
		i++
		if i == opt.BatchSize {
			i = 0
			opt.Response <- OutputLogMessage{
				OutputMessage: OutputMessage{
					Code:    http.StatusOK,
					Message: "OK",
				},
				Data: list,
			}
		}
	}
	if i > 0 {
		opt.Response <- OutputLogMessage{
			OutputMessage: OutputMessage{
				Code:    http.StatusOK,
				Message: "OK",
			},
			Data: list[0:i],
		}
	}
	log.Printf("Found %d log messages\n", totalMessage)
	if err := rows.Err(); err != nil {
		opt.Response <- OutputLogMessage{
			OutputMessage: OutputMessage{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			},
		}
		return err
	}
	opt.Response <- OutputLogMessage{
		OutputMessage: OutputMessage{
			Code:    http.StatusNoContent,
			Message: "OK",
		},
	}
	return nil
}

func (c *Connection) Insert(logs []LogEntry) error {
	tx, err := c.conn.Begin()
	if err != nil {
		return errors.New(fmt.Sprintf("open click-house tx get error %v", err))
	}
	insertScript := fmt.Sprintf(`INSERT INTO %s.%s(id, tag, timestamp, date, container_name, level, message, context.key, context.value) VALUES (?,?,?,?,?,?,?,?,?)`, DatabaseName, LogTableName)
	stmt, err := tx.Prepare(insertScript)
	if err != nil {
		return errors.New(fmt.Sprintf("prepare click-house statement get error %v", err))
	}
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
