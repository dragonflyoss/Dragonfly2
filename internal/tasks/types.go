package tasks

type PreheatRequest struct {
	URL      string            `json:"url" binding:"required"`
	Tag      string            `json:"tag" binding:"required"`
	Digest   string            `json:"digest" binding:"required"`
	Filter   string            `json:"filter" binding:"omitempty"`
	Headers  map[string]string `json:"headers" binding:"omitempty"`
}

type PreheatResponse struct{
	Success bool `json:"success"`
}
