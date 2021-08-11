package oauth

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/github"
	"gorm.io/gorm"
)

type githubOauth2 struct {
	baseOauth2
}

func NewGithubOauth2(name string, clientID string, clientSecret string, db *gorm.DB) (Oauther, error) {

	oa := &githubOauth2{}
	oa.Name = name
	oa.Config = &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Scopes: []string{"https://www.googleapis.com/auth/userinfo.profile",
			"https://www.googleapis.com/auth/userinfo.email"},
		Endpoint: github.Endpoint,
	}
	oa.UserInfoURL = "https://api.github.com/user"

	redirectURL, err := oa.GetRediectURL(db)
	if err != nil {
		return nil, err
	}
	oa.Config.RedirectURL = redirectURL
	return oa, nil
}

func (oa *githubOauth2) GetOauthUserInfo(token string) (*oauth2User, error) {
	req, err := http.NewRequest("GET", oa.UserInfoURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "token"+" "+token)
	response, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	u := oauth2User{}
	err = json.Unmarshal(contents, &u)
	if err != nil {
		return nil, err
	}
	return &u, nil
}
