package main

import (
	"errors"
	. "hermes/clickhouse"
	. "hermes/core"
)

type DriverClickHouse struct {
	Pool *CHPool
}

func init() {
	drivers["clickhouse"] = &DriverClickHouse{}
}

func (d *DriverClickHouse) Open(config DriverConfig) (err error) {
	d.Pool, err = CreateCHPool(0, 1, 1000, "")
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
