package model

import (
	"gorm.io/datatypes"
)

type Scheduler struct {
	Model
	Name               string              `gorm:"column:name;size:256;uniqueIndex;not null" json:"name"`
	BIO                string              `gorm:"column:bio;size:1024" json:"bio"`
	Config             datatypes.JSONMap   `gorm:"column:config;not null" json:"config"`
	ClientConfig       datatypes.JSONMap   `gorm:"column:client_config;not null" json:"client_config"`
	CDNs               []CDN               `gorm:"many2many:cdn_scheduler;" json:"cdns"`
	SchedulerInstances []SchedulerInstance `json:"scheduler_instances"`
}
