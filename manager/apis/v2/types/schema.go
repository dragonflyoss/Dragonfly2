package types

type SchedulerCluster struct {
	ClusterId       string `json:"cluster_id,size:63" binding:"omitempty"`
	SchedulerConfig string `json:"scheduler_config,size:4095" binding:"required"`
	ClientConfig    string `json:"client_config,size:4095" binding:"required"`
	Version         int64  `json:"version" binding:"omitempty"`
	Creator         string `json:"creator,size:31" binding:"omitempty"`
	Modifier        string `json:"modifier,size:31" binding:"omitempty"`
	CreatedAt       string `json:"created_at" binding:"omitempty"`
	UpdatedAt       string `json:"updated_at" binding:"omitempty"`
}

type SchedulerClusterUri struct {
	ClusterId string `uri:"id" binding:"required"`
}

type ListSchedulerClustersResponse struct {
	Clusters []*SchedulerCluster `json:"clusters"`
}

type SchedulerInstance struct {
	InstanceId     string `json:"instance_id,size:63" binding:"omitempty"`
	ClusterId      string `json:"cluster_id,size:63" binding:"required"`
	SecurityDomain string `json:"security_domain,size:63" binding:"required"`
	Vips           string `json:"vips,size:4095" binding:"omitempty"`
	Idc            string `json:"idc,size:63" binding:"required"`
	Location       string `json:"location,size:4095" binding:"omitempty"`
	NetConfig      string `json:"net_config,size:4095" binding:"omitempty"`
	HostName       string `json:"host_name,size:63" binding:"required"`
	Ip             string `json:"ip,size:31" binding:"required"`
	Port           int32  `json:"port" binding:"required"`
	State          string `json:"state,size:15" binding:"omitempty"`
	Version        int64  `json:"version" binding:"omitempty"`
	CreatedAt      string `json:"created_at" binding:"omitempty"`
	UpdatedAt      string `json:"updated_at" binding:"omitempty"`
}

type ListSchedulerInstancesResponse struct {
	Instances []*SchedulerInstance `json:"instances"`
}

type SchedulerInstanceUri struct {
	InstanceId string `uri:"id" binding:"required"`
}

type CdnCluster struct {
	ClusterId string `json:"cluster_id,size:63" binding:"omitempty"`
	Config    string `json:"config,size:4095" binding:"required"`
	Version   int64  `json:"version" binding:"omitempty"`
	Creator   string `json:"creator,size:31" binding:"omitempty"`
	Modifier  string `json:"modifier,size:31" binding:"omitempty"`
	CreatedAt string `json:"created_at" binding:"omitempty"`
	UpdatedAt string `json:"updated_at" binding:"omitempty"`
}

type ListCdnClustersResponse struct {
	Clusters []*CdnCluster `json:"clusters"`
}

type CdnClusterUri struct {
	ClusterId string `uri:"id" binding:"required"`
}

type CdnInstance struct {
	InstanceId string `json:"instance_id,size:63" binding:"omitempty"`
	ClusterId  string `json:"cluster_id,size:63" binding:"required"`
	Idc        string `json:"idc,size:63" binding:"required"`
	Location   string `json:"location,size:4095" binding:"omitempty"`
	HostName   string `json:"host_name,size:63" binding:"required"`
	Ip         string `json:"ip,size:31" binding:"required"`
	Port       int32  `json:"port" binding:"required"`
	RpcPort    int32  `json:"rpc_port" binding:"required"`
	DownPort   int32  `json:"down_port" binding:"required"`
	State      string `json:"state,size:15" binding:"omitempty"`
	Version    int64  `json:"version" binding:"omitempty"`
	CreatedAt  string `json:"created_at" binding:"omitempty"`
	UpdatedAt  string `json:"updated_at" binding:"omitempty"`
}

type ListCdnInstancesResponse struct {
	Instances []*CdnInstance `json:"instances"`
}

type CdnInstanceUri struct {
	InstanceId string `uri:"id" binding:"required"`
}

type SecurityDomain struct {
	SecurityDomain string `json:"security_domain,size:63" binding:"required"`
	DisplayName    string `json:"display_name,size:63" binding:"required"`
	ProxyDomain    string `json:"proxy_domain,size:4095" binding:"omitempty"`
	Version        int64  `json:"version" binding:"omitempty"`
	Creator        string `json:"creator,size:31" binding:"omitempty"`
	Modifier       string `json:"modifier,size:31" binding:"omitempty"`
	CreatedAt      string `json:"created_at" binding:"omitempty"`
	UpdatedAt      string `json:"updated_at" binding:"omitempty"`
}

type ListSecurityDomainsResponse struct {
	Domains []*SecurityDomain `json:"domains"`
}

type ListQuery struct {
	Marker       int `form:"marker" binding:"omitempty"`
	MaxItemCount int `form:"maxItemCount" binding:"omitempty,min=10,max=50"`
}

type TaskUri struct {
	Uri   string `json:"uri,size:1023"`
	State string `json:"state,size:15"`
}

type WarmupTask struct {
	TaskId      string     `json:"task_id,size:63"`
	ClusterId   string     `json:"cluster_id,size:63"`
	Type        string     `json:"type,size:31"`
	OriginalUri string     `json:"original_uri,size:1023"`
	State       string     `json:"state,size:15"`
	TaskUris    []*TaskUri `json:"task_uris"`
	CreatedAt   string     `json:"created_at"`
	UpdatedAt   string     `json:"updated_at"`
}
