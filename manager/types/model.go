package types

import "encoding/json"

type ModelParams struct {
	ID        string `uri:"id" binding:"required"`
	VersionID string `uri:"version_id" binding:"omitempty"`
}

type Model struct {
	Hostname           string  `json:"hostname" binding:"required"`
	IP                 string  `json:"ip" binding:"required"`
	SchedulerClusterID uint    `json:"scheduler_cluster_id" binding:"required"`
	Recall             float64 `json:"recall" binding:"omitempty"`
	Precision          float64 `json:"precision" binding:"omitempty"`
	VersionID          string  `json:"version_id" binding:"omitempty"`
}

type StorageModel struct {
	ID      string `json:"id" binding:"required"`      //模型对应的scheduler的id
	Params  []byte `json:"params" binding:"required"`  //线形模型的参数
	Version string `json:"version" binding:"required"` //模型版本号
}

type Version struct {
	VersionID string  `json:"version_id" binding:"required"`
	Precision float64 `json:"precision" binding:"required"`
	Recall    float64 `json:"recall" binding:"required"`
}

func (m StorageModel) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m Version) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}
