package types

type TaskRegisterRequest struct {
	Headers map[string]string `json:"headers,omitempty"`
	URL     string            `json:"rawURL,omitempty"`
	TaskID  string            `json:"taskId,omitempty"`
	Md5     string
}