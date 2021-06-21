package model

import (
	"gorm.io/datatypes"
)

const (
	SchedulerStatusActive   = "active"
	SchedulerStatusInactive = "inactive"
)

type SchedulerInstance struct {
	Model
	VIPs            string            `gorm:"column:vips;size:1024;not null" json:"vips"`
	IDC             string            `gorm:"column:idc;size:1024;not null" json:"idc"`
	Location        string            `gorm:"column:location;size:1024" json:"location"`
	NetConfig       datatypes.JSONMap `gorm:"column:net_config;not null" json:"net_config"`
	Host            string            `gorm:"column:host;size:256;uniqueIndex;not null" json:"host"`
	IP              string            `gorm:"column:ip;size:256;not null" json:"ip"`
	Port            int32             `gorm:"column:port;not null" json:"port"`
	Status          string            `gorm:"type:enum('active', 'inactive');default:'inactive'" json:"status"`
	SchedulerID     *uint             `json:"scheduler_id"`
	Scheduler       Scheduler         `json:"scheduler"`
	SecurityGroupID *uint             `json:"scheduler_group_id"`
	SecurityGroup   SecurityGroup     `json:"scheduler_group"`
}
