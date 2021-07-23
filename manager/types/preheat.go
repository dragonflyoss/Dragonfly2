package types

type CreatePreheatRequest struct {
	Type    string            `json:"type" binding:"required,oneof=image file"`
	URL     string            `json:"url" binding:"required"`
	Filter  string            `json:"filter" binding:"omitempty"`
	Headers map[string]string `json:"headers" binding:"omitempty"`
}
