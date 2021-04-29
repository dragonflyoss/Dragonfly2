package types

import (
	"time"
)

type SchedulerCluster struct {
	ClusterId       string    `json:"cluster_id,size:63"`
	SchedulerConfig string    `json:"scheduler_config,size:4095"`
	ClientConfig    string    `json:"client_config,size:4095"`
	Version         int64     `json:"version"`
	Creator         string    `json:"creator,size:31"`
	Modifier        string    `json:"modifier,size:31"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type ListSchedulerClustersResponse struct {
	Clusters []*SchedulerCluster `json:"clusters"`
}

type SchedulerInstance struct {
	InstanceId     string    `json:"instance_id,size:63"`
	ClusterId      string    `json:"cluster_id,size:63"`
	SecurityDomain string    `json:"security_domain,size:63"`
	Vips           string    `json:"vips,size:4095"`
	Idc            string    `json:"idc,size:63"`
	Location       string    `json:"location,size:4095"`
	NetConfig      string    `json:"net_config,size:4095"`
	HostName       string    `json:"host_name,size:63"`
	Ip             string    `json:"ip,size:31"`
	Port           int32     `json:"port"`
	State          string    `json:"state,size:15"`
	Version        int64     `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CdnCluster struct {
	ClusterId string    `json:"cluster_id,size:63"`
	Config    string    `json:"config,size:4095"`
	Version   int64     `json:"version"`
	Creator   string    `json:"creator,size:31"`
	Modifier  string    `json:"modifier,size:31"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CdnInstance struct {
	InstanceId string    `json:"instance_id,size:63"`
	ClusterId  string    `json:"cluster_id,size:63"`
	Idc        string    `json:"idc,size:63"`
	Location   string    `json:"location,size:4095"`
	HostName   string    `json:"host_name,size:63"`
	Ip         string    `json:"ip,size:31"`
	Port       int32     `json:"port"`
	RpcPort    int32     `json:"rpc_port"`
	DownPort   int32     `json:"down_port"`
	State      string    `json:"state,size:15"`
	Version    int64     `json:"version"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type SecurityDomain struct {
	SecurityDomain string    `json:"security_domain,size:63"`
	DisplayName    string    `json:"display_name,size:63"`
	ProxyDomain    string    `json:"proxy_domain,size:4095"`
	Version        int64     `json:"version"`
	Creator        string    `json:"creator,size:31"`
	Modifier       string    `json:"modifier,size:31"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
