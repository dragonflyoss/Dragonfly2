package types

type SchedulerInstanceParams struct {
	ID string `uri:"id" binding:"required,gte=1,lte=32"`
}

type CreateSchedulerInstanceRequest struct {
	SchedulerID    string   `json:"schduler_id" binding:"required"`
	SecurityDomain string   `json:"security_domain" binding:"required"`
	VIPs           []string `json:"vips" binding:"omitempty"`
	IDC            string   `json:"idc" binding:"required"`
	Location       string   `json:"location" binding:"omitempty"`
	NetConfig      string   `json:"net_config" binding:"omitempty"`
	Host           string   `json:"host" binding:"required"`
	IP             string   `json:"ip" binding:"required"`
	Port           int32    `json:"port" binding:"required"`
}

type UpdateSchedulerInstanceRequest struct {
	SchedulerID    string   `json:"schduler_id" binding:"omitempty"`
	SecurityDomain string   `json:"security_domain" binding:"omitempty"`
	VIPs           []string `json:"vips" binding:"omitempty"`
	IDC            string   `json:"idc" binding:"omitempty"`
	Location       string   `json:"location" binding:"omitempty"`
	NetConfig      string   `json:"net_config" binding:"omitempty"`
	Host           string   `json:"host" binding:"omitempty"`
	IP             string   `json:"ip" binding:"omitempty"`
	Port           int32    `json:"port" binding:"omitempty"`
}

type GetSchedulerInstancesQuery struct {
	Page    int `json:"page" binding:"omitempty,min=1"`
	PerPage int `json:"per_page" binding:"omitempty,max=50"`
}
