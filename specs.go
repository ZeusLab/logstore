package main

type LogCollector interface {
	Collect() error
}

type LogStorage interface {
	Persist() error
	FindAllTag() ([]string, error)
	FindAllTagHistories() ([]string, error)
	FindLogByTagAndHistory() ([]LogEntry, error)
}
