package types

type SchedulerCluster struct {
	ClusterID    string `json:"cluster_id" binding:"omitempty"`
	Config       string `json:"config" binding:"required"`
	ClientConfig string `json:"client_config" binding:"required"`
	Version      int64  `json:"version" binding:"omitempty"`
	Creator      string `json:"creator" binding:"omitempty"`
	Modifier     string `json:"modifier" binding:"omitempty"`
	CreatedAt    string `json:"created_at" binding:"omitempty"`
	UpdatedAt    string `json:"updated_at" binding:"omitempty"`
}

type SchedulerClusterURI struct {
	ClusterID string `uri:"id" binding:"required"`
}

type ListSchedulerClustersResponse struct {
	Clusters []*SchedulerCluster `json:"clusters"`
}

type SchedulerInstance struct {
	InstanceID     string `json:"instance_id" binding:"omitempty"`
	ClusterID      string `json:"cluster_id" binding:"required"`
	SecurityDomain string `json:"security_domain" binding:"required"`
	VIPs           string `json:"vips" binding:"omitempty"`
	IDC            string `json:"idc" binding:"required"`
	Location       string `json:"location" binding:"omitempty"`
	NetConfig      string `json:"net_config" binding:"omitempty"`
	HostName       string `json:"host_name" binding:"required"`
	IP             string `json:"ip" binding:"required"`
	Port           int32  `json:"port" binding:"required"`
	State          string `json:"state" binding:"omitempty"`
	Version        int64  `json:"version" binding:"omitempty"`
	CreatedAt      string `json:"created_at" binding:"omitempty"`
	UpdatedAt      string `json:"updated_at" binding:"omitempty"`
}

type ListSchedulerInstancesResponse struct {
	Instances []*SchedulerInstance `json:"instances"`
}

type SchedulerInstanceURI struct {
	InstanceID string `uri:"id" binding:"required"`
}

type ListCDNClustersResponse struct {
	Clusters []*CDNCluster `json:"clusters"`
}

type CDNClusterURI struct {
	ClusterID string `uri:"id" binding:"required"`
}

type CDNInstance struct {
	InstanceID string `json:"instance_id" binding:"omitempty"`
	ClusterID  string `json:"cluster_id" binding:"required"`
	IDC        string `json:"idc" binding:"required"`
	Location   string `json:"location" binding:"omitempty"`
	HostName   string `json:"host_name" binding:"required"`
	IP         string `json:"ip" binding:"required"`
	Port       int32  `json:"port" binding:"required"`
	RPCPort    int32  `json:"rpc_port" binding:"required"`
	DownPort   int32  `json:"down_port" binding:"required"`
	State      string `json:"state" binding:"omitempty"`
	Version    int64  `json:"version" binding:"omitempty"`
	CreatedAt  string `json:"created_at" binding:"omitempty"`
	UpdatedAt  string `json:"updated_at" binding:"omitempty"`
}

type ListCDNInstancesResponse struct {
	Instances []*CDNInstance `json:"instances"`
}

type CDNInstanceURI struct {
	InstanceID string `uri:"id" binding:"required"`
}

type SecurityDomain struct {
	SecurityDomain string `json:"security_domain" binding:"required"`
	DisplayName    string `json:"display_name" binding:"required"`
	ProxyDomain    string `json:"proxy_domain" binding:"omitempty"`
	Version        int64  `json:"version" binding:"omitempty"`
	Creator        string `json:"creator" binding:"omitempty"`
	Modifier       string `json:"modifier" binding:"omitempty"`
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

type TaskURI struct {
	URI   string `json:"uri"`
	State string `json:"state"`
}

type WarmupTask struct {
	TaskID      string     `json:"task_id"`
	ClusterID   string     `json:"cluster_id"`
	Type        string     `json:"type"`
	OriginalURI string     `json:"original_uri"`
	State       string     `json:"state"`
	TaskURIs    []*TaskURI `json:"task_uris"`
	CreatedAt   string     `json:"created_at"`
	UpdatedAt   string     `json:"updated_at"`
}
