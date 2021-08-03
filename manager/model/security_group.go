package model

type SecurityGroup struct {
	Model
	Name              string             `gorm:"column:name;type:varchar(256);uniqueIndex;not null" json:"name"`
	BIO               string             `gorm:"column:bio;type:varchar(1024)" json:"bio"`
	Domain            string             `gorm:"column:domain;type:varchar(256);uniqueIndex;not null" json:"domain"`
	ProxyDomain       string             `gorm:"column:proxy_domain;type:varchar(1024)" json:"proxy_domain"`
	SchedulerClusters []SchedulerCluster `json:"-"`
	CDNClusters       []CDNCluster       `json:"-"`
}
