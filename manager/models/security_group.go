package models

import (
	"gorm.io/gorm"
)

type SecurityGroup struct {
	gorm.Model
	Name               string `gorm:"column:name;uniqueIndex;not null"`
	BIO                string `gorm:"column:bio;size:1024"`
	Domain             string `gorm:"column:domain;uniqueIndex;not null"`
	ProxyDomain        string `gorm:"column:proxy_domain"`
	SchedulerInstances []SchedulerInstance
}
