package core

type LogDriver interface {
	Open(config DriverConfig) error
	Collect(messages []InputLogPayload) error
	FindAllTag() ([]string, error)
	FetchingLog(opt QueryLogOption) ([]LogEntry, error)
	Close() error
}

type QueryLogOption struct {
	Tag       string
	LogLevel  int32
	StartTime int64
	EndTime   int64
	LastId    int64
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
