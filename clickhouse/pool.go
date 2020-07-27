package clickhouse

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"log"
	"sync"
	"time"
)

/**
┌─────id──────┬─tag─┬──timestamp──┬───date───┬─container name──┬──level─┬─message─────────────┬─context.key───┬─context.value────┐
│ 1234567890  │ app │ 12345678901 │ 20200507 │    container    │  info  │ This is log message │ ['a','b','c'] │ ['v1','v2','v3'] │
└─────────────┴─────┴─────────────┴──────────┴─────────────────┴────────┴─────────────────────┴───────────────┴──────────────────┘
 */

const (
	DatabaseName = "hermes"
	LogTableName = "logs"
)

var (
	errPoolBusy     = errors.New("pool too busy")
	errPoolClosed   = errors.New("pool is closed")
	errorMissingTag = errors.New("missing tag")
)

/**
ClickHouse connection pool
 */
type CHPool struct {
	sync.Mutex
	pool          chan *Connection
	dsn           string
	maxActive     int
	maxLifeTime   int64
	minActive     int
	currentActive int
	currentInUsed int
	isClosed      bool
}

func CreateCHPool(min, max int, maxLifeTime int64, dsn string) (*CHPool, error) {
	if max < min {
		return nil, errors.New("max must be larger than min")
	}

	if min < 0 || max <= 0 {
		return nil, errors.New("number of active connection must larger than zero")
	}

	chPool := &CHPool{
		dsn:           dsn,
		minActive:     min,
		maxActive:     max,
		currentActive: 0,
		currentInUsed: 0,
		pool:          make(chan *Connection, max),
		maxLifeTime:   maxLifeTime,
		isClosed:      false,
	}

	for i := 0; i < min; i++ {
		_, err := chPool.openConnection()
		if err != nil {
			return nil, err
		}
	}

	go chPool.scheduleToCloseInActiveConnection()
	return chPool, nil
}

func (p *CHPool) scheduleToCloseInActiveConnection() {
	t := time.NewTicker(5 * time.Second)
	for {
		<-t.C
		if !p.closeInActiveConnection() {
			t.Stop()
			break
		}
	}
}

func (p *CHPool) closeInActiveConnection() bool {
	p.Lock()
	defer p.Unlock()
	if p.isClosed {
		return false
	}
	now := time.Now().Unix()
	for ; ; {
		if p.currentActive == 0 {
			break
		}

		c, err := p.getAvailableConn(100 * time.Millisecond)
		if c == nil || err != nil {
			break
		}

		if now-c.ts > p.maxLifeTime {
			log.Println("close inactive connection to ClickHouse database")
			_ = c.Close()
			p.currentActive--
			continue
		}

		_ = p.releaseConn(c)
		break
	}
	return true
}

func (p *CHPool) getAvailableConn(t time.Duration) (conn *Connection, err error) {
	select {
	case conn = <-p.pool:
		p.currentInUsed++
		err = nil
		break
	case <-time.After(t):
		conn = nil
		err = errPoolBusy
		break
	}
	return
}

func (p *CHPool) releaseConn(conn *Connection) error {
	if p.isClosed {
		_ = conn.Close()
		p.currentActive--
		return nil
	}
	conn.ts = time.Now().Unix()
	p.pool <- conn
	p.currentInUsed--
	return nil
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
	p.currentActive ++
	return c, nil
}

func (p *CHPool) AcquireWithTimeout(t time.Duration) (conn *Connection, err error) {
	p.Lock()
	defer p.Unlock()
	if p.isClosed {
		err = errPoolClosed
		return
	}

	attempt := 1
	for ; attempt <= 5; attempt++ {
		conn, err = p.getAvailableConn(t)
		if conn != nil {
			break
		}
		if err != nil && p.currentActive < p.maxActive {
			conn, err = p.openConnection()
			break
		}
	}

	if attempt == 5 {
		log.Println("Out of connection pool. It seems there are many connections that are taken over than 500ms")
		err = errors.New("can not connect to database")
	}

	if conn != nil {
		conn.ts = time.Now().Unix()
	}
	return
}

func (p *CHPool) Acquire() (conn *Connection, err error) {
	return p.AcquireWithTimeout(100 * time.Millisecond)
}

func (p *CHPool) Release(conn *Connection) error {
	p.Lock()
	defer p.Unlock()
	return p.releaseConn(conn)
}

func (p *CHPool) Close() error {
	p.Lock()
	defer p.Unlock()
	p.isClosed = true
	for i := 0; p.currentActive > 0 && i < p.maxActive; i++ {
		conn, err := p.getAvailableConn(1 * time.Second)
		if err != nil || conn == nil {
			break
		}
		if conn != nil {
			_ = conn.Close()
			p.currentActive--
		}
	}
	return nil
}
