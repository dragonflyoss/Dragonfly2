package model

import (
	"gorm.io/datatypes"
)

type CDN struct {
	Model
	Name         string            `gorm:"column:name;size:256;uniqueIndex;not null" json:"name"`
	BIO          string            `gorm:"column:bio;size:1024" json:"bio"`
	Config       datatypes.JSONMap `gorm:"column:config;not null" json:"config"`
	Schedulers   []Scheduler       `gorm:"many2many:cdn_scheduler;" json:"schedulers"`
	CDNInstances []CDNInstance     `json:"cdn_instanaces"`
}
