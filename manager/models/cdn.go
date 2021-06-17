package models

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type CDN struct {
	gorm.Model
	Name         string            `gorm:"column:name;uniqueIndex;not null"`
	BIO          string            `gorm:"column:bio;size:1024"`
	Config       datatypes.JSONMap `gorm:"column:config;not null"`
	Schedulers   []Scheduler       `gorm:"many2many:cdn_scheduler;"`
	CDNInstances []CDNInstance
}
