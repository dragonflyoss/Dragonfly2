package types

type CDNParams struct {
	ID uint `uri:"id" binding:"required"`
}

type CreateCDNRequest struct {
	Host                string `json:"host" binding:"required"`
	SecurityGroupDomain string `json:"security_group_domain" binding:"omitempty"`
	IDC                 string `json:"idc" binding:"required"`
	Location            string `json:"location" binding:"omitempty"`
	IP                  string `json:"ip" binding:"required"`
	Port                int32  `json:"port" binding:"required"`
	DownloadPort        int32  `json:"download_port" binding:"required"`
}

type UpdateCDNRequest struct {
	SecurityGroupDomain string `json:"security_group_domain" binding:"omitempty"`
	IDC                 string `json:"idc" binding:"omitempty"`
	Location            string `json:"location" binding:"omitempty"`
	IP                  string `json:"ip" binding:"omitempty"`
	Port                int32  `json:"port" binding:"omitempty"`
	DownloadPort        int32  `json:"download_port" binding:"omitempty"`
	CDNID               *uint  `json:"cdn_id" binding:"omitempty"`
}

type GetCDNsQuery struct {
	Host         string `form:"host" binding:"omitempty"`
	IDC          string `form:"idc" binding:"omitempty"`
	Location     string `form:"location" binding:"omitempty"`
	IP           string `form:"ip" binding:"omitempty"`
	Port         int32  `form:"port" binding:"omitempty"`
	DownloadPort int32  `form:"download_port" binding:"omitempty"`
	Page         int    `form:"page" binding:"omitempty,gte=1"`
	PerPage      int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
	Status       string `form:"status" binding:"omitempty,oneof=active inactive"`
}
