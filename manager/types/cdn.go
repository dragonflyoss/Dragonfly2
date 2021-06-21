package types

type CDNParams struct {
	ID string `uri:"id" binding:"required,gte=1,lte=32"`
}

type AddInstanceToCDNParams struct {
	ID         string `uri:"id" binding:"required,gte=1,lte=32"`
	InstanceID string `uri:"instance_id" binding:"required,gte=1,lte=32"`
}

type AddSchedulerToCDNParams struct {
	ID          string `uri:"id" binding:"required,gte=1,lte=32"`
	SchedulerID string `uri:"scheduler_id" binding:"required,gte=1,lte=32"`
}

type CreateCDNRequest struct {
	Name   string                 `json:"name" binding:"required"`
	BIO    string                 `json:"bio" binding:"omitempty"`
	Config map[string]interface{} `json:"config" binding:"required"`
}

type UpdateCDNRequest struct {
	Name   string                 `json:"name" binding:"omitempty"`
	BIO    string                 `json:"bio" binding:"omitempty"`
	Config map[string]interface{} `json:"config" binding:"omitempty"`
}

type GetCDNsQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
