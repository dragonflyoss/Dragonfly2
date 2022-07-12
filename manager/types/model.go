package types

type ModelParams struct {
	ID        string `uri:"id" binding:"required"`
	VersionId string `uri:"version_id" binding:"omitempty"`
}

type ModelInfos struct {
	Hostname           string             `json:"hostname" binding:"required"`
	IP                 string             `json:"ip" binding:"required"`
	SchedulerClusterID uint               `json:"scheduler_cluster_id" binding:"required"`
	Params             map[string]float64 `json:"params" binding:"omitempty"`
	Recall             float64            `json:"recall" binding:"omitempty"`
	Precision          float64            `json:"precision" binding:"omitempty"`
	VersionId          string             `json:"version_id" binding:"omitempty"`
}
