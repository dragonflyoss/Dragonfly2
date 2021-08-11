package model

const (
	SchedulerStatusActive   = "active"
	SchedulerStatusInactive = "inactive"
)

type Scheduler struct {
	Model
	HostName           string           `gorm:"column:host_name;type:varchar(256);index:uk_scheduler,unique;not null;comment:hostname" json:"host_name"`
	VIPs               string           `gorm:"column:vips;type:varchar(1024);comment:virtual ip address" json:"vips"`
	IDC                string           `gorm:"column:idc;type:varchar(1024);comment:internet data center" json:"idc"`
	Location           string           `gorm:"column:location;type:varchar(1024);comment:location" json:"location"`
	NetConfig          JSONMap          `gorm:"column:net_config;comment:network configuration" json:"net_config"`
	IP                 string           `gorm:"column:ip;type:varchar(256);not null;comment:ip address" json:"ip"`
	Port               int32            `gorm:"column:port;not null;comment:grpc service listening port" json:"port"`
	Status             string           `gorm:"column:status;type:varchar(256);default:'inactive';comment:service status" json:"status"`
	SchedulerClusterID *uint            `gorm:"index:uk_scheduler,unique;not null;comment:scheduler cluster id"`
	SchedulerCluster   SchedulerCluster `json:"-"`
}
