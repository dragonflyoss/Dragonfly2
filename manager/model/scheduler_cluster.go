package model

type SchedulerCluster struct {
	Model
	Name            string       `gorm:"column:name;type:varchar(256);uniqueIndex;not null" json:"name"`
	BIO             string       `gorm:"column:bio;type:varchar(1024)" json:"bio"`
	Config          JSONMap      `gorm:"column:config;not null" json:"config"`
	ClientConfig    JSONMap      `gorm:"column:client_config;not null" json:"client_config"`
	Scopes          JSONMap      `gorm:"column:scopes" json:"scopes"`
	IsDefault       bool         `gorm:"column:is_default;not null;default:false" json:"is_default"`
	CDNClusters     []CDNCluster `gorm:"many2many:cdn_cluster_scheduler_cluster;" json:"-"`
	Schedulers      []Scheduler  `json:"-"`
	SecurityGroupID *uint
	SecurityGroup   SecurityGroup `json:"-"`
}
