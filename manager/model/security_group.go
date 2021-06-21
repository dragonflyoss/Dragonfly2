package model

type SecurityGroup struct {
	Model
	Name               string              `gorm:"column:name;size:256;uniqueIndex;not null" json:"name"`
	BIO                string              `gorm:"column:bio;size:1024" json:"bio"`
	Domain             string              `gorm:"column:domain;size:256;uniqueIndex;not null" json:"domain"`
	ProxyDomain        string              `gorm:"column:proxy_domain;size:1024" json:"proxy_domain"`
	SchedulerInstances []SchedulerInstance `json:"scheduler_instances"`
	CDNInstances       []CDNInstance       `json:"cdn_instances"`
}
