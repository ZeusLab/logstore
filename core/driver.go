package core

import "context"

type LogDriver interface {
	Open(config DriverConfig) error
	Collect(messages []InputLogPayload) error
	FindAllTag(ctx context.Context) ([]string, error)
	FetchingLog(ctx context.Context, opt QueryLogOption) error
	Close() error
}

type QueryLogOption struct {
	Tag       string
	LogLevel  int32
	StartTime int64
	EndTime   int64
	LastId    int64
	BatchSize int32
	Response  chan OutputLogMessage
}

type LogEntry struct {
	Id            int64
	Tag           string
	Timestamp     int64
	Date          string
	ContainerName string
	Level         int32
	Message       string
	ContextKeys   []string
	ContextValues []string
}
