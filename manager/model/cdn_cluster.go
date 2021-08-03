package model

type CDNCluster struct {
	Model
	Name              string             `gorm:"column:name;type:varchar(256);uniqueIndex;not null" json:"name"`
	BIO               string             `gorm:"column:bio;type:varchar(1024)" json:"bio"`
	Config            JSONMap            `gorm:"column:config;not null" json:"config"`
	SchedulerClusters []SchedulerCluster `gorm:"many2many:cdn_cluster_scheduler_cluster;" json:"-"`
	CDNs              []CDN              `json:"-"`
	SecurityGroupID   *uint
	SecurityGroup     SecurityGroup `json:"-"`
}
