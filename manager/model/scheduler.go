package model

const (
	SchedulerStatusActive   = "active"
	SchedulerStatusInactive = "inactive"
)

type Scheduler struct {
	Model
	HostName           string  `gorm:"column:host_name;type:varchar(256);uniqueIndex;not null" json:"host_name"`
	VIPs               string  `gorm:"column:vips;type:varchar(1024)" json:"vips"`
	IDC                string  `gorm:"column:idc;type:varchar(1024)" json:"idc"`
	Location           string  `gorm:"column:location;type:varchar(1024)" json:"location"`
	NetConfig          JSONMap `gorm:"column:net_config" json:"net_config"`
	IP                 string  `gorm:"column:ip;type:varchar(256);not null" json:"ip"`
	Port               int32   `gorm:"column:port;not null" json:"port"`
	Status             string  `gorm:"column:status;type:varchar(256);default:'inactive'" json:"status"`
	SchedulerClusterID *uint
	SchedulerCluster   SchedulerCluster `json:"-"`
}
