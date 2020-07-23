package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"log"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

/**
┌─application─┬───timestamp───┬────date────┬────container name────┬────container id────┬─message─────┐
│ postgresql  │ 1588842127114 │ 20200507   │ /athena_postgres_1   │ 1f5552dba15dd0df   │ Hello world │
└─────────────┴───────────────┴────────────┴──────────────────────┘────────────────────┘─────────────┘
 */

const (
	DatabaseName = "hermes"
	LogTableName = "hermes.logs"
)

var (
	errPoolBusy   = errors.New("pool too busy")
	errPoolClosed = errors.New("pool is closed")
)

type LogEntry struct {
	Id            int64
	Tag           string
	Timestamp     int64
	Date          string
	ContainerName *string
	Level         string
	Message       string
	ContextKeys   []string
	ContextValues []string
}

/**
ClickHouse connection pool
 */

type Connection struct {
	closed bool
	conn   *sql.DB
	ts     int64
}

func (c *Connection) close() error {
	c.closed = true
	c.ts = 0
	return c.conn.Close()
}

type CHPool struct {
	sync.Mutex
	pool        chan *Connection
	dsn         string
	maxActive   int
	maxLifeTime int64
	minActive   int
	nowActive   int
	isClosed    bool
}

func CreateCHPool(min, max int, maxLifeTime int64, dsn string) (*CHPool, error) {
	if max < min {
		return nil, errors.New("max must be larger than min")
	}

	if min < 0 || max <= 0 {
		return nil, errors.New("number of active connection must larger than zero")
	}

	chPool := &CHPool{
		dsn:         dsn,
		minActive:   min,
		maxActive:   max,
		nowActive:   0,
		pool:        make(chan *Connection, max),
		maxLifeTime: maxLifeTime,
		isClosed:    false,
	}

	for i := 0; i < min; i++ {
		_, err := chPool.openConnection()
		if err != nil {
			return nil, err
		}
	}

	go chPool.closeInActiveConnection()
	return chPool, nil
}

func (p *CHPool) closeInActiveConnection() {
	t := time.NewTicker(1 * time.Second)
	for {
		<-t.C
		now := time.Now().Unix()
		for ; ; {
			if p.nowActive == 0 {
				break
			}
			c, err := p.acquireWithTimeout(100 * time.Millisecond)
			if err != nil || c == nil {
				break
			}

			if now-c.ts > p.maxLifeTime {
				_ = c.close()
				p.nowActive--
				continue
			}

			_ = p.release(c)
			break
		}
	}
}

func (p *CHPool) openConnection() (*Connection, error) {
	connect, err := sql.Open("clickhouse", p.dsn)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not connect to click-house db %v", err))
	}

	if err := connect.Ping(); err != nil {
		_ = connect.Close()
		if exception, ok := err.(*clickhouse.Exception); ok {
			return nil, errors.New(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		}
		return nil, err
	}
	c := &Connection{
		closed: false,
		conn:   connect,
		ts:     time.Now().Unix(),
	}
	p.pool <- c
	p.nowActive ++
	return c, nil
}

func (p *CHPool) acquireWithTimeout(t time.Duration) (conn *Connection, err error) {
	p.Lock()
	defer p.Unlock()
	if p.isClosed {
		err = errPoolClosed
		return
	}
	select {
	case conn = <-p.pool:
		err = nil
		break
	case <-time.After(t):
		conn = nil
		err = errPoolBusy
		break
	}
	if err != nil && p.nowActive < p.maxActive {
		conn, err = p.openConnection()
	}
	conn.ts = time.Now().Unix()
	return
}

func (p *CHPool) acquire() (conn *Connection, err error) {
	return p.acquireWithTimeout(30 * time.Second)
}

func (p *CHPool) release(conn *Connection) error {
	p.Lock()
	defer p.Unlock()
	if p.isClosed {
		_ = conn.close()
		return nil
	}
	if p.nowActive >= p.maxActive {
		_ = conn.close()
		return nil
	}
	conn.ts = time.Now().Unix()
	p.pool <- conn
	p.nowActive--
	return nil
}

func (p *CHPool) close() error {
	p.Lock()
	defer p.Unlock()
	p.isClosed = true
	return nil
}

// end of ClickHouse connection pool

type RetreiveLogOption struct {
	Limit         int
	Id            int64
	Application   string
	Date          string
	Head          bool
	Greps         []string
	ContainerName *string
	ContainerId   *string
}

var errorMissingTag = errors.New("missing tag")

func createOption(values url.Values) (*RetreiveLogOption, error) {
	opt := RetreiveLogOption{
		Limit:         100,
		Head:          false,
		Id:            0,
		ContainerName: nil,
		ContainerId:   nil,
	}
	tagValues := values["tag"]
	if tagValues == nil || len(tagValues) == 0 {
		return nil, errorMissingTag
	}
	opt.Application = tagValues[0]
	limits := values["limit"]
	if limits != nil && len(limits) > 0 {
		v, err := strconv.Atoi(limits[0])
		if err != nil {
			return nil, err
		}
		opt.Limit = v
	}

	dateValues := values["date"]
	opt.Date = time.Now().Format("20060102")
	if dateValues != nil && len(dateValues) > 0 {
		opt.Date = dateValues[0]
	}

	ids := values["id"]
	if ids != nil && len(ids) > 0 {
		v, err := strconv.Atoi(ids[0])
		if err != nil {
			return nil, err
		}
		opt.Id = int64(v)
	}

	if values["is_head"] != nil && len(values["is_head"]) > 0 {
		opt.Head, _ = strconv.ParseBool(values["is_head"][0])
	}

	if values["container_name"] != nil && len(values["container_name"]) > 0 {
		opt.ContainerName = &values["container_name"][0]
	}

	if values["container_id"] != nil && len(values["container_id"]) > 0 {
		opt.ContainerId = &values["container_id"][0]
	}

	opt.Greps = values["greps"]
	return &opt, nil
}

//func getClickHouseUrl() string {
//	return fmt.Sprintf("tcp://%s?debug=%v", clickHouseAddress, debug)
//}

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

func selectAppHistories(application string) ([]string, error) {
	connect, err := openConnection()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not open connection to click-house db %v", err))
	}
	defer func() {
		_ = connect.Close()
	}()

	log.Println(`select distinct(date) from hermes.logs where application = ? order by date desc with parameter: `, application)
	rows, err := connect.Query(`select distinct(date) from hermes.logs where application = ? order by date desc`, application)
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

func selectDistinctApplication() ([]string, error) {
	connect, err := openConnection()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not open connection to click-house db %v", err))
	}
	defer func() {
		_ = connect.Close()
	}()

	log.Println(`query select distinct(application) from hermes.logs`)
	rows, err := connect.Query(`select distinct(application) from hermes.logs`)
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

func selectLogWithOpt(option RetreiveLogOption) ([]LogMessage, error) {
	connect, err := openConnection()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not open connection to click-house db %v", err))
	}
	defer func() {
		_ = connect.Close()
	}()

	//create parameters
	parameters := make([]interface{}, 0)
	parameters = append(parameters, option.Application)
	parameters = append(parameters, option.Date)

	//build grep condition
	grepCondition := ""
	if option.Greps != nil && len(option.Greps) > 0 {
		s := make([]string, 0)
		for i := 0; i < len(option.Greps); i++ {
			s = append(s, fmt.Sprintf("positionCaseInsensitive(message, ?) > 0"))
		}
		grepCondition = fmt.Sprintf("AND (%s)", strings.Join(s, " AND "))
	}

	if option.Greps != nil && len(option.Greps) > 0 {
		parameters = append(parameters, option.Greps)
	}

	//build container condition
	containers := make([]string, 0)
	if option.ContainerName != nil {
		containers = append(containers, "container_name = ?")
		parameters = append(parameters, *option.ContainerName)
	}

	if option.ContainerId != nil {
		containers = append(containers, "container_id = ?")
		parameters = append(parameters, *option.ContainerId)
	}

	containerCondition := ""
	if len(containers) > 0 {
		containerCondition = fmt.Sprintf("AND (%s)", strings.Join(containers, " AND "))
	}

	//build id condition
	idCondition := ""
	if option.Head {
		idCondition = "id > ?"
	} else {
		idCondition = "id < ?"
		if option.Id == 0 {
			idCondition = "id > ?"
		}
	}
	parameters = append(parameters, option.Id)

	// build limit and order
	ordered := "DESC"
	if option.Head {
		ordered = "ASC"
	}
	parameters = append(parameters, option.Limit)

	// create query
	query := fmt.Sprintf(`SELECT id, container_id, container_name, timestamp, message FROM hermes.logs
		WHERE (application = ? AND date = ?) %s %s
		AND %s
		ORDER BY timestamp %s
		LIMIT ?`, grepCondition, containerCondition, idCondition, ordered)

	log.Printf(`query %s 
with params %+v
`, query, parameters)

	rows, err := connect.Query(query, parameters...)
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
			IdStr:         fmt.Sprintf("%d", id),
			Tag:           option.Application,
			Date:          option.Date,
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
