package types

type SchedulerParams struct {
	ID string `uri:"id" binding:"required,gte=1,lte=32"`
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
	Page    int `json:"page" binding:"omitempty,min=1"`
	PerPage int `json:"per_page" binding:"omitempty,max=50"`
}
