package task

type PreheatRequest struct {
	URL     string
	Digest  string
	Filter  string
	Headers map[string]string
	BizID   string
}

type PreheatResponse struct{}
