package types

type Config struct {
	Id       string `json:"id"`
	Object   string `json:"object" binding:"required"`
	Type     string `json:"type" binding:"required"`
	Version  uint64 `json:"version" binding:"required"`
	Data     []byte `json:"data" binding:"required"`
	CreateAt string `json:"create_at"`
	UpdateAt string `json:"update_at"`
}

type ClientConfig struct {
}

type CdnConfig struct {
}

type SchedulerConfig struct {
	ClientConfig *ClientConfig `json:"client_config"`
	CdnHosts     []*ServerInfo `json:"cdn_hosts"`
}

type ServerInfo struct {
	HostInfo *HostInfo `json:"host_info"`
	RpcPort  int32     `json:"rpc_port"`
	DownPort int32     `json:"down_port"`
}

type HostInfo struct {
	Ip             string `json:"ip"`
	HostName       string `json:"host_name"`
	SecurityDomain string `json:"security_domain"`
	Location       string `json:"location"`
	Idc            string `json:"idc"`
	NetTopology    string `json:"net_topology"`
}

type AddConfigResponse struct {
	Id string `json:"id"`
}

type GetConfigResponse struct {
	Config *Config `json:"config"`
}

type ListConfigsResponse struct {
	Configs []*Config `json:"configs"`
}
