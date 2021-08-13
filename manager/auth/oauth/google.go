package oauth

import (
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"gorm.io/gorm"
)

type googleOauth2 struct {
	baseOauth2
}

func NewGoogleOauth2(name string, clientID string, clientSecret string, db *gorm.DB) (Oauther, error) {

	oa := &googleOauth2{}
	oa.Name = name
	oa.Config = &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes:       strings.Split(GoogleScopes, ","),
		Endpoint:     google.Endpoint,
	}
	oa.UserInfoURL = GithubUserInfoURL

	redirectURL, err := oa.GetRediectURL(db)
	if err != nil {
		return nil, err
	}
	oa.Config.RedirectURL = redirectURL
	return oa, nil
}
