package model

const (
	CDNStatusActive   = "active"
	CDNStatusInactive = "inactive"
)

type CDN struct {
	Model
	HostName     string `gorm:"column:host_name;type:varchar(256);uniqueIndex;not null" json:"host_name"`
	IDC          string `gorm:"column:idc;type:varchar(1024)" json:"idc"`
	Location     string `gorm:"column:location;type:varchar(1024)" json:"location"`
	IP           string `gorm:"column:ip;type:varchar(256);not null" json:"ip"`
	Port         int32  `gorm:"column:port;not null" json:"port"`
	DownloadPort int32  `gorm:"column:download_port;not null" json:"download_port"`
	Status       string `gorm:"column:status;type:varchar(256);default:'inactive'" json:"status"`
	CDNClusterID *uint
	CDNCluster   CDNCluster `json:"-"`
}
