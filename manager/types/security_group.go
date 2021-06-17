package types

type SecurityGroupParams struct {
	ID string `uri:"id" binding:"required,gte=1,lte=32"`
}

type CreateSecurityGroupRequest struct {
	Name        string `json:"name" binding:"required"`
	Domain      string `json:"domain" binding:"required"`
	ProxyDomain string `json:"proxy_domain" binding:"omitempty"`
}

type UpdateSecurityGroupRequest struct {
	Name        string `json:"name" binding:"omitempty"`
	Domain      string `json:"domain" binding:"omitempty"`
	ProxyDomain string `json:"proxy_domain" binding:"omitempty"`
}

type GetSecurityGroupsQuery struct {
	Page    int `json:"page" binding:"omitempty,min=1"`
	PerPage int `json:"per_page" binding:"omitempty,max=50"`
}
