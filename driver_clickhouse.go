package main

import (
	"errors"
	"fmt"
	. "hermes/clickhouse"
	. "hermes/core"
	"strconv"
	"strings"
)

type DriverClickHouse struct {
	Pool *CHPool
}

func init() {
	drivers["clickhouse"] = &DriverClickHouse{}
}

func (d *DriverClickHouse) Open(config DriverConfig) (err error) {
	minActiveConn := 0
	maxActiveConn := 1
	maxInActiveTime := int64(300000)

	dbAddress := ""
	dbOptions := make([]string, 0)
	for _, opt := range config.Options {
		parts := strings.Split(opt, "=")
		if len(parts) < 2 {
			err = fmt.Errorf("option of clickhouse is wrong format. value = %s", opt)
			return
		}

		key := parts[0]
		value := strings.Join(parts[1:], "=")
		switch key {
		case "minActiveConn":
			minActiveConn, err = strconv.Atoi(value)
			if err != nil {
				return
			}
			break
		case "maxActiveConn":
			maxActiveConn, err = strconv.Atoi(value)
			if err != nil {
				return
			}
			break
		case "maxInActiveTime":
			maxInActiveTime, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return
			}
			break
		case "address":
			dbAddress = value
			break
		case "dsnopts":
			dbOptions = append(dbOptions, value)
			break
		default:
			break
		}
	}

	if StrIsEmpty(dbAddress) {
		err = errors.New("missing address of clickhouse database")
		return
	}

	dsn := fmt.Sprintf("tcp://%s", dbAddress)
	if len(dbOptions) > 0 {
		dsn = fmt.Sprintf("tcp://%s?%s", dbAddress, strings.Join(dbOptions, "&"))
	}
	d.Pool, err = CreateCHPool(minActiveConn, maxActiveConn, maxInActiveTime, dsn)
	if err != nil {
		return
	}
	return
}

func (d *DriverClickHouse) Collect(messages []InputLogPayload) error {
	c, err := d.Pool.Acquire()
	if err != nil {
		return err
	}

	if c == nil {
		return errors.New("can not acquire connection")
	}

	defer func() {
		_ = d.Pool.Release(c)
	}()

	entries := make([]LogEntry, len(messages))

	for i, v := range messages {
		entries[i] = LogEntry{
			Tag:           v.Tag,
			Timestamp:     v.Timestamp,
			ContainerName: v.ContainerName,
			Message:       v.Message,
			Level:         v.Level,
			ContextKeys:   v.Context.Keys(),
			ContextValues: v.Context.Values(),
		}
	}

	return c.Insert(entries)
}

func (d *DriverClickHouse) FindAllTag() ([]string, error) {
	c, err := d.Pool.Acquire()
	if err != nil {
		return nil, err
	}

	if c == nil {
		return nil, errors.New("can not acquire connection")
	}

	defer func() {
		_ = d.Pool.Release(c)
	}()

	return c.GetAllTags()
}

func (d *DriverClickHouse) Close() error {
	return d.Pool.Close()
}
