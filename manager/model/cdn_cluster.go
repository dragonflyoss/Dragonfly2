package model

import (
	"gorm.io/datatypes"
)

type CDNCluster struct {
	Model
	Name              string             `gorm:"column:name;size:256;uniqueIndex;not null" json:"name"`
	BIO               string             `gorm:"column:bio;size:1024" json:"bio"`
	Config            datatypes.JSONMap  `gorm:"column:config;not null" json:"config"`
	SchedulerClusters []SchedulerCluster `gorm:"many2many:cdn_cluster_scheduler_cluster;" json:"-"`
	CDNs              []CDN              `json:"-"`
}
