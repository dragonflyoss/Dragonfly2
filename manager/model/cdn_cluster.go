package model

type CDNCluster struct {
	Model
	Name              string             `gorm:"column:name;type:varchar(256);index:uk_cdn_cluster_name,unique;not null;comment:name" json:"name"`
	BIO               string             `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Config            JSONMap            `gorm:"column:config;not null;comment:configuration" json:"config"`
	SchedulerClusters []SchedulerCluster `gorm:"many2many:cdn_cluster_scheduler_cluster;" json:"-"`
	IsDefault         bool               `gorm:"column:is_default;not null;default:false;comment:default cdn cluster" json:"is_default"`
	CDNs              []CDN              `json:"-"`
	SecurityGroupID   uint               `gorm:"comment:security group id"`
	SecurityGroup     SecurityGroup      `json:"-"`
}
