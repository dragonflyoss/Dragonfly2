package model

import (
	"gorm.io/datatypes"
)

type SchedulerCluster struct {
	Model
	Name            string            `gorm:"column:name;size:256;uniqueIndex;not null" json:"name"`
	BIO             string            `gorm:"column:bio;size:1024" json:"bio"`
	Config          datatypes.JSONMap `gorm:"column:config;not null" json:"config"`
	ClientConfig    datatypes.JSONMap `gorm:"column:client_config;not null" json:"client_config"`
	Scopes          datatypes.JSONMap `gorm:"column:scopes" json:"scopes"`
	IsDefault       bool              `gorm:"column:is_default;not null;default:false" json:"is_default"`
	CDNClusters     []CDNCluster      `gorm:"many2many:cdn_cluster_scheduler_cluster;" json:"-"`
	Schedulers      []Scheduler       `json:"-"`
	SecurityGroupID *uint
	SecurityGroup   SecurityGroup `json:"-"`
}
