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
	LogTableName = "hermes.logs"
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
	p.Lock()
	defer p.Unlock()
	t := time.NewTicker(1 * time.Second)
	for {
		<-t.C
		if p.isClosed {
			t.Stop()
			break
		}
		now := time.Now().Unix()
		for ; ; {
			if p.nowActive == 0 {
				break
			}

			var c *Connection
			select {
			case c = <-p.pool:
				p.nowActive --
				break
			case <-time.After(100 * time.Millisecond):
				c = nil
				break
			}
			if c == nil {
				break
			}

			if now-c.ts > p.maxLifeTime {
				log.Println("close inactive connection to ClickHouse database")
				_ = c.Close()
				continue
			}

			_ = p.Release(c)
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

func (p *CHPool) AcquireWithTimeout(t time.Duration) (conn *Connection, err error) {
	p.Lock()
	defer p.Unlock()
	if p.isClosed {
		err = errPoolClosed
		return
	}
	select {
	case conn = <-p.pool:
		p.nowActive --
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

func (p *CHPool) Acquire() (conn *Connection, err error) {
	return p.AcquireWithTimeout(30 * time.Second)
}

func (p *CHPool) Release(conn *Connection) error {
	p.Lock()
	defer p.Unlock()
	if p.isClosed {
		_ = conn.Close()
		return nil
	}
	if p.nowActive >= p.maxActive {
		_ = conn.Close()
		return nil
	}
	conn.ts = time.Now().Unix()
	p.pool <- conn
	p.nowActive++
	return nil
}

func (p *CHPool) Close() error {
	p.Lock()
	defer p.Unlock()
	p.isClosed = true
	for i := 0; p.nowActive > 0 && i < p.maxActive; i++ {
		select {
		case conn := <-p.pool:
			_ = conn.Close()
			p.nowActive--
			break
		case <-time.After(200 * time.Millisecond):
			break
		}
	}
	return nil
}
