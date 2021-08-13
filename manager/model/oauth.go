package model

type Oauth struct {
	Model
	Name         string `gorm:"column:name;type:varchar(256);index:uk_oauth_name,unique;not null;comment:oauth name" json:"name"`
	ClientID     string `gorm:"column:client_id;type:varchar(256);index:uk_oauth_client_id,unique;not null;comment:client id for oauth" json:"client_id"`
	ClientSecret string `gorm:"column:client_secret;type:varchar(1024);comment:client secret for oauth" json:"client_secret"`
	// scope list split by ','
	Scopes      string `gorm:"column:scopes;type:varchar(1024);comment:scopes" json:"scopes"`
	UserInfoURL string `gorm:"column:user_info_url;type:varchar(256);not null;comment:user info url" json:"user_info_url"`
	AuthURL     string `gorm:"column:auth_url;type:varchar(256);not null;comment:auth url" json:"auth_url"`
	TokenURL    string `gorm:"column:token_url;type:varchar(256);not null;comment:token url" json:"token_url"`
}
