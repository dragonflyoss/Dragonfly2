package types

type OauthParams struct {
	ID uint `uri:"id" binding:"required"`
}

type OauthPathParams struct {
	OauthName string `uri:"oauth_name" binding:"required"`
}

type OauthBaseRquest struct {
	Name         string `json:"name" binding:"required"`
	ClientID     string `json:"client_id" binding:"required"`
	ClientSecret string `json:"client_secret" binding:"required"`
	// scope list split by ','
	Scopes   string `json:"scopes" binding:"omitempty"`
	AuthURL  string `json:"auth_url" binding:"omitempty"`
	TokenURL string `json:"token_url" binding:"omitempty"`
}

type CreateOauthRequest struct {
	OauthBaseRquest
}

type UpdateOauthRequest struct {
	OauthBaseRquest
}

type GetOauthsQuery struct {
	Name    string `json:"name" binding:"required"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
