package models

import (
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	SchedulerStatusActive   = "active"
	SchedulerStatusInactive = "inactive"
)

type SchedulerInstance struct {
	gorm.Model
	VIPs            string            `gorm:"column:vips;not null"`
	IDC             string            `gorm:"column:idc;not null"`
	Location        string            `gorm:"column:location;size:1024"`
	NetConfig       datatypes.JSONMap `gorm:"column:net_config;not null"`
	Host            string            `gorm:"column:host;uniqueIndex;not null"`
	IP              string            `gorm:"column:ip;not null"`
	Port            int32             `gorm:"column:port;not null"`
	Status          string            `gorm:"type:enum('active', 'inactive');default:'inactive'"`
	SchedulerID     uint
	Scheduler       Scheduler
	SecurityGroupID uint
	SecurityGroup   SecurityGroup
}
