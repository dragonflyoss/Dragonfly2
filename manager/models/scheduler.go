package models

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type Scheduler struct {
	gorm.Model
	Name               string            `gorm:"column:name;uniqueIndex;not null"`
	BIO                string            `gorm:"column:bio;size:1024"`
	Config             datatypes.JSONMap `gorm:"column:config;not null"`
	ClientConfig       datatypes.JSONMap `gorm:"column:client_config;not null"`
	CDNs               []CDN             `gorm:"many2many:cdn_scheduler;"`
	SchedulerInstances []SchedulerInstance
}
