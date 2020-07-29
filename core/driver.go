package core

type LogDriver interface {
	Open(config DriverConfig) error
	Collect(messages []InputLogPayload) error
	FindAllTag() ([]string, error)
	Close() error
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
