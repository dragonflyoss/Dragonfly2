package types

type CDNInstanceParams struct {
	ID string `uri:"id" binding:"required,gte=1,lte=32"`
}

type CreateCDNInstanceRequest struct {
	CDNID        string `json:"cdn_id" binding:"required"`
	IDC          string `json:"idc" binding:"required"`
	Location     string `json:"location" binding:"omitempty"`
	Host         string `json:"host" binding:"required"`
	IP           string `json:"ip" binding:"required"`
	Port         int32  `json:"port" binding:"required"`
	DownloadPort int32  `json:"download_port" binding:"required"`
}

type UpdateCDNInstanceRequest struct {
	CDNID        string `json:"cdn_id" binding:"omitempty"`
	IDC          string `json:"idc" binding:"omitempty"`
	Location     string `json:"location" binding:"omitempty"`
	Host         string `json:"host" binding:"omitempty"`
	IP           string `json:"ip" binding:"omitempty"`
	Port         int32  `json:"port" binding:"omitempty"`
	DownloadPort int32  `json:"download_port" binding:"omitempty"`
}

type GetCDNInstancesQuery struct {
	Page    int `json:"page" binding:"omitempty,min=1"`
	PerPage int `json:"per_page" binding:"omitempty,max=50"`
}
