package oauth

import (
	"fmt"
	"strings"

	"d7y.io/dragonfly/v2/manager/model"
	"golang.org/x/oauth2"
	"gorm.io/gorm"
)

type oauth2 struct {
	Name        string
	UserInfoURL string
	Config      *oauth2.Config
}

// oauth interface
type Oauther interface {
	GetRediectURL(*gorm.DB) (string, error)
	GetOauthUserInfo(string) (*model.User, error)
}

func NewOauth(name string, clientID string, clientSecret string, scopes string, authURL string, tokenURL string, db *gorm.DB) (*oauth, error) {

	oa := &oauth2{
		Name: name,
		Config: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       strings.Split(scopes, ","),
			Endpoint: oauth2.Endpoint{
				AuthURL:  authURL,
				TokenURL: tokenURL,
			},
		},
	}
	redirectURL, err := oa.GetRediectURL(db)
	if err != nil {
		return nil, err
	}
	oa.Config.RedirectURL = redirectURL
	return oa, nil
}

func (o *oauth2) GetRediectURL(db *gorm.DB) (string, error) {

	s := model.Settings{}
	if err := db.First(&s, model.Settings{
		Key: "server_domain",
	}).Error; err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/api/v1/oauth/%s/sigin", s.Value, o.Name), nil
}

func (o *oauth2) GetOauthUserInfo(code string) (*model.User, error) {
	user := model.User{}
	return &user, nil
}
