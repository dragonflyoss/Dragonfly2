package types

type CreateCDNInstanceRequest struct {
	CDNID        string `json:"cdn_id" binding:"required"`
	IDC          string `json:"idc" binding:"required"`
	Location     string `json:"location" binding:"omitempty"`
	Host         string `json:"host" binding:"required"`
	IP           string `json:"ip" binding:"required"`
	Port         int32  `json:"port" binding:"required"`
	RPCPort      int32  `json:"rpc_port" binding:"required"`
	DownloadPort int32  `json:"download_port" binding:"required"`
	CreatedAt    string `json:"created_at" binding:"omitempty"`
	UpdatedAt    string `json:"updated_at" binding:"omitempty"`
}
