package models

import (
	"gorm.io/gorm"
)

const (
	CDNStatusActive   = "active"
	CDNStatusInactive = "inactive"
)

type CDNInstance struct {
	gorm.Model
	IDC          string `gorm:"column:idc;size:1024;not null"`
	Location     string `gorm:"column:location;size:1024"`
	Host         string `gorm:"column:host;size:256;uniqueIndex;not null"`
	IP           string `gorm:"column:ip;size:256;not null"`
	Port         int32  `gorm:"column:port;not null"`
	DownloadPort int32  `gorm:"column:download_port;not null"`
	Status       string `gorm:"type:enum('active', 'inactive');default:'inactive'"`
	CDNID        uint
	CDN          CDN
}
