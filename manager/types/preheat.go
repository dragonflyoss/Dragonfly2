package types

import "time"

type PreheatParams struct {
	ID string `uri:"id" binding:"required"`
}

type CreatePreheatRequest struct {
	SchedulerClusterID string            `json:"scheduler_cluster_id" binding:"omitempty"`
	Type               string            `json:"type" binding:"required,oneof=image file"`
	URL                string            `json:"url" binding:"required"`
	Filter             string            `json:"filter" binding:"omitempty"`
	Headers            map[string]string `json:"headers" binding:"omitempty"`
}

type Preheat struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"create_at"`
}
