package types

type CDNClusterParams struct {
	ID uint `uri:"id" binding:"required"`
}

type AddCDNToCDNClusterParams struct {
	ID    uint `uri:"id" binding:"required"`
	CDNID uint `uri:"cdn_id" binding:"required"`
}

type AddSchedulerClusterToCDNClusterParams struct {
	ID                 uint `uri:"id" binding:"required"`
	SchedulerClusterID uint `uri:"scheduler_cluster_id" binding:"required"`
}

type CreateCDNClusterRequest struct {
	Name   string                 `json:"name" binding:"required"`
	BIO    string                 `json:"bio" binding:"omitempty"`
	Config map[string]interface{} `json:"config" binding:"required"`
}

type UpdateCDNClusterRequest struct {
	Name   string                 `json:"name" binding:"omitempty"`
	BIO    string                 `json:"bio" binding:"omitempty"`
	Config map[string]interface{} `json:"config" binding:"omitempty"`
}

type GetCDNClustersQuery struct {
	Name    string `form:"name" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
