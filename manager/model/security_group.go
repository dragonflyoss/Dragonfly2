package model

type SecurityGroup struct {
	Model
	Name              string             `gorm:"column:name;type:varchar(256);index:uk_security_group_name,unique;not null;comment:name" json:"name"`
	BIO               string             `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	Domain            string             `gorm:"column:domain;type:varchar(256);index:uk_security_group_domain,unique;not null;comment:domain" json:"domain"`
	ProxyDomain       string             `gorm:"column:proxy_domain;type:varchar(1024);comment:proxy domain" json:"proxy_domain"`
	SchedulerClusters []SchedulerCluster `json:"-"`
	CDNClusters       []CDNCluster       `json:"-"`
}
