package core

/** Input */
type InputLogPayload struct {
	Tag           string          `json:"tag,omitempty"`
	Timestamp     int64           `json:"timestamp,omitempty"`
	ContainerName string          `json:"container_name,omitempty"`
	Level         string          `json:"level,omitempty"`
	Message       string          `json:"message,omitempty"`
	Context       InputLogContext `json:"context,omitempty"`
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
	Code    int32  `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type OutputTagMessage struct {
	OutputMessage
	Data []string `json:"data,omitempty"`
}

type OutputLogPayload struct {
	Id    int64  `json:"id,omitempty"`
	IdStr string `json:"id_str,omitempty"`
	Date  string `json:"date,omitempty"`
	InputLogPayload
}
