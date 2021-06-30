package types

type SchedulerClusterParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddSchedulerToSchedulerClusterParams struct {
	ID          uint `uri:"id" binding:"required"`
	SchedulerID uint `uri:"scheduler_id" binding:"required"`
}

type CreateSchedulerClusterRequest struct {
	Name                string                 `json:"name" binding:"required"`
	BIO                 string                 `json:"bio" binding:"omitempty"`
	Config              map[string]interface{} `json:"config" binding:"required"`
	ClientConfig        map[string]interface{} `json:"client_config" binding:"required"`
	SecurityGroupDomain string                 `json:"security_group_domain" binding:"omitempty"`
}

type UpdateSchedulerClusterRequest struct {
	Name                string                 `json:"name" binding:"omitempty"`
	BIO                 string                 `json:"bio" binding:"omitempty"`
	Config              map[string]interface{} `json:"config" binding:"omitempty"`
	ClientConfig        map[string]interface{} `json:"client_config" binding:"omitempty"`
	SecurityGroupDomain string                 `json:"security_group_domain" binding:"omitempty"`
}

type GetSchedulerClustersQuery struct {
	Name    string `form:"name" binding:"required"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
