package model

type SchedulerCluster struct {
	Model
	Name            string        `gorm:"column:name;type:varchar(256);index:uk_scheduler_cluster_name,unique;not null;comment:name" json:"name"`
	BIO             string        `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Config          JSONMap       `gorm:"column:config;not null;comment:configuration" json:"config"`
	ClientConfig    JSONMap       `gorm:"column:client_config;not null;comment:client configuration" json:"client_config"`
	Scopes          JSONMap       `gorm:"column:scopes;comment:match scopes" json:"scopes"`
	IsDefault       bool          `gorm:"column:is_default;not null;default:false;comment:default scheduler" json:"is_default"`
	CDNClusters     []CDNCluster  `gorm:"many2many:cdn_cluster_scheduler_cluster;" json:"-"`
	Schedulers      []Scheduler   `json:"-"`
	SecurityGroupID uint          `gorm:"comment:security group id"`
	SecurityGroup   SecurityGroup `json:"-"`
}
