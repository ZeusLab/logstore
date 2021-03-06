package core

import "strings"

/** Input */
type InputLogPayload struct {
	Tag           string          `json:"tag,omitempty"`
	Timestamp     int64           `json:"timestamp,omitempty"`
	ContainerName string          `json:"container_name,omitempty"`
	Level         string          `json:"level,omitempty"`
	Message       string          `json:"message,omitempty"`
	Context       InputLogContext `json:"context,omitempty"`
}

const (
	LevelAll       string = "ALL"
	LevelDefault   string = "DEFAULT"
	LevelDebug     string = "DEBUG"
	LevelInfo      string = "INFO"
	LevelNotice    string = "NOTICE"
	LevelWarning   string = "WARN"
	LevelError     string = "ERROR"
	LevelCritical  string = "CRITICAL"
	LevelAlert     string = "ALERT"
	LevelEmergency string = "EMERGENCY"

	LevelAllInt       int32 = 0
	LevelDebugInt     int32 = 100
	LevelInfoInt      int32 = 200
	LevelNoticeInt    int32 = 300
	LevelWarningInt   int32 = 400
	LevelErrorInt     int32 = 500
	LevelCriticalInt  int32 = 600
	LevelAlertInt     int32 = 700
	LevelEmergencyInt int32 = 800
)

func LogLevelStr(level int32) string {
	switch level {
	case LevelDebugInt:
		return LevelDebug
	case LevelInfoInt:
		return LevelInfo
	case LevelNoticeInt:
		return LevelNotice
	case LevelWarningInt:
		return LevelWarning
	case LevelErrorInt:
		return LevelError
	case LevelCriticalInt:
		return LevelCritical
	case LevelAlertInt:
		return LevelAlert
	case LevelEmergencyInt:
		return LevelEmergency
	case LevelAllInt:
	default:
		return LevelAll
	}
	return LevelAll
}

func LogLevelInt(level string) int32 {
	level = strings.ToUpper(level)
	switch level {
	case LevelDebug:
		return LevelDebugInt
	case LevelInfo:
		return LevelInfoInt
	case LevelNotice:
		return LevelNoticeInt
	case LevelWarning:
		return LevelWarningInt
	case LevelError:
		return LevelErrorInt
	case LevelCritical:
		return LevelCriticalInt
	case LevelAlert:
		return LevelAlertInt
	case LevelEmergency:
		return LevelEmergencyInt
	case LevelAll,
		LevelDefault:
		return LevelAllInt
	}
	return LevelAllInt
}

type InputLogContext map[string]string

func (m InputLogContext) Keys() []string {
	if m == nil {
		return []string{}
	}
	rs := make([]string, 0, len(m))
	for k := range m {
		rs = append(rs, k)
	}
	return rs
}

func (m InputLogContext) Values() []string {
	if m == nil {
		return []string{}
	}
	rs := make([]string, 0, len(m))
	for _, v := range m {
		rs = append(rs, v)
	}
	return rs
}

/** Output */
type OutputMessage struct {
	Code int32  `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type OutputTagMessage struct {
	OutputMessage
	Data []string `json:"data,omitempty"`
}

type OutputLogMessage struct {
	OutputMessage
	Data []OutputLogPayload `json:"data,omitempty"`
}

type OutputLogPayload struct {
	Id    int64  `json:"id,omitempty"`
	IdStr string `json:"id_str,omitempty"`
	Date  string `json:"date,omitempty"`
	InputLogPayload
}
