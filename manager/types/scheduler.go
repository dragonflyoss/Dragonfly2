package types

type SchedulerParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddInstanceToSchedulerParams struct {
	ID         uint `uri:"id" binding:"required"`
	InstanceID uint `uri:"instance_id" binding:"required"`
}

type CreateSchedulerRequest struct {
	Name         string                 `json:"name" binding:"required"`
	BIO          string                 `json:"bio" binding:"omitempty"`
	Config       map[string]interface{} `json:"config" binding:"required"`
	ClientConfig map[string]interface{} `json:"client_config" binding:"required"`
}

type UpdateSchedulerRequest struct {
	Name         string                 `json:"name" binding:"omitempty"`
	BIO          string                 `json:"bio" binding:"omitempty"`
	Config       map[string]interface{} `json:"config" binding:"omitempty"`
	ClientConfig map[string]interface{} `json:"client_config" binding:"omitempty"`
}

type GetSchedulersQuery struct {
	Name    string `form:"name" binding:"required"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
