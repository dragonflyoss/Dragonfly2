package model

type Oauth struct {
	Model
	Name         string `gorm:"column:name;type:varchar(256);index:uk_oauth_name,unique;not null;comment:oauth name" json:"name"`
	BIO          string `gorm:"column:bio;type:varchar(1024);comment:biography" json:"bio"`
	ClientID     string `gorm:"column:client_id;type:varchar(256);index:uk_oauth_client_id,unique;not null;comment:client id for oauth" json:"client_id"`
	ClientSecret string `gorm:"column:client_secret;type:varchar(1024);comment:client secret for oauth" json:"client_secret"`
}
