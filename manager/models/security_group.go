package models

import (
	"gorm.io/gorm"
)

type SecurityGroup struct {
	gorm.Model
	Name               string `gorm:"column:name;size:256;uniqueIndex;not null"`
	BIO                string `gorm:"column:bio;size:1024"`
	Domain             string `gorm:"column:domain;size:256;uniqueIndex;not null"`
	ProxyDomain        string `gorm:"column:proxy_domain;size:1024"`
	SchedulerInstances []SchedulerInstance
}
