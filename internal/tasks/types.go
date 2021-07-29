package tasks

//TODO: check if required or omitempty
type PreheatRequest struct {
	URL      string            `json:"url" binding:"required"`
	Tag      string            `json:"tag" binding:"required"`
	Digest   string            `json:"digest" binding:"omitempty"`
	Filter   string            `json:"filter" binding:"omitempty"`
	Headers  map[string]string `json:"headers" binding:"omitempty"`
}

type PreheatResponse struct{

}
