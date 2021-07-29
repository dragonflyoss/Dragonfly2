package tasks

type PreheatRequest struct {
	URL     string            `json:"url" validate:"required,url"`
	Tag     string            `json:"tag" validate:"required"`
	Digest  string            `json:"digest" validate:"omitempty"`
	Filter  string            `json:"filter" validate:"omitempty"`
	Headers map[string]string `json:"headers" validate:"omitempty"`
}

type PreheatResponse struct {
}
