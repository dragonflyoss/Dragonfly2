package model

const (
	CDNStatusActive   = "active"
	CDNStatusInactive = "inactive"
)

type CDN struct {
	Model
	HostName     string     `gorm:"column:host_name;type:varchar(256);index:uk_cdn,unique;not null;comment:hostname" json:"host_name"`
	IDC          string     `gorm:"column:idc;type:varchar(1024);comment:internet data center" json:"idc"`
	Location     string     `gorm:"column:location;type:varchar(1024);comment:location" json:"location"`
	IP           string     `gorm:"column:ip;type:varchar(256);not null;comment:ip address" json:"ip"`
	Port         int32      `gorm:"column:port;not null;comment:grpc service listening port" json:"port"`
	DownloadPort int32      `gorm:"column:download_port;not null;comment:download service listening port" json:"download_port"`
	Status       string     `gorm:"column:status;type:varchar(256);default:'inactive';comment:service status" json:"status"`
	CDNClusterID uint       `gorm:"index:uk_cdn,unique;not null;comment:cdn cluster id"`
	CDNCluster   CDNCluster `json:"-"`
}
