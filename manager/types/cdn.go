package types

type CDNParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddInstanceToCDNParams struct {
	ID         uint `uri:"id" binding:"required"`
	InstanceID uint `uri:"instance_id" binding:"required"`
}

type AddSchedulerToCDNParams struct {
	ID          uint `uri:"id" binding:"required"`
	SchedulerID uint `uri:"scheduler_id" binding:"required"`
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
