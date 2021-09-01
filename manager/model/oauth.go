package model

type Oauth struct {
	Model
	Name         string `gorm:"column:name;type:varchar(256);index:uk_oauth2_name,unique;not null;comment:oauth2 name" json:"name"`
	BIO          string `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	ClientID     string `gorm:"column:client_id;type:varchar(256);index:uk_oauth2_client_id,unique;not null;comment:client id for oauth2" json:"client_id"`
	ClientSecret string `gorm:"column:client_secret;type:varchar(1024);not null;comment:client secret for oauth2" json:"client_secret"`
	RedirectURL  string `gorm:"column:redirect_url;type:varchar(1024);comment:authorization callback url" json:"redirect_url"`
}
