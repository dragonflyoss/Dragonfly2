package tasks

type PreheatRequest struct {
	URL     string
	Tag     string
	Digest  string
	Filter  string
	Headers map[string]string
}

type PreheatResponse struct{}
