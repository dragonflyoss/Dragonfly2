package oauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/oauth2"
	"gorm.io/gorm"
)

const (
	Google            = "google"
	Github            = "github"
	GoogleScopes      = "https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/userinfo.profile"
	GoogleUserInfoURL = "https://www.googleapis.com/oauth2/v2/userinfo"

	GithubScopes      = "user,public_repo"
	GithubUserInfoURL = "https://api.github.com/user"
)

type baseOauth2 struct {
	Name        string
	UserInfoURL string
	Config      *oauth2.Config
}

type oauth2User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// oauth interface
type Oauther interface {
	GetRediectURL(*gorm.DB) (string, error)
	GetOauthUserInfo(string) (*oauth2User, error)
	ExchangeTokenByCode(string) (string, error)
	OauthLinkUser(*oauth2User, *gorm.DB) (*model.User, error)
	AuthCodeURL() string
}

func New(oauth *model.Oauth, db *gorm.DB) (Oauther, error) {
	var o Oauther
	var err error
	switch oauth.Name {
	case Google:
		o, err = NewGoogleOauth2(oauth.Name, oauth.ClientID, oauth.ClientSecret, db)
	case Github:
		o, err = NewGithubOauth2(oauth.Name, oauth.ClientID, oauth.ClientSecret, db)
	default:
		o, err = NewBaseOauth2(oauth.Name, oauth.ClientID, oauth.ClientSecret, oauth.Scopes, oauth.AuthURL, oauth.TokenURL, db)
	}
	if err != nil {
		return nil, err
	}
	return o, nil

}

func NewBaseOauth2(name string, clientID string, clientSecret string, scopes string, authURL string, tokenURL string, db *gorm.DB) (Oauther, error) {

	oa := &baseOauth2{
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

func (oa *baseOauth2) GetRediectURL(db *gorm.DB) (string, error) {

	s := model.Settings{}
	if err := db.First(&s, model.Settings{
		Key: "server_domain",
	}).Error; err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/api/v1/oauth/callback/%s", s.Value, oa.Name), nil
}

func (oa *baseOauth2) AuthCodeURL() string {
	return oa.Config.AuthCodeURL(stringutils.RandString(5))
}

func (oa *baseOauth2) GetOauthUserInfo(token string) (*oauth2User, error) {
	response, err := http.Get(fmt.Sprintf("%s?access_token=%s", oa.UserInfoURL, token))
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

func (oa *baseOauth2) ExchangeTokenByCode(code string) (string, error) {
	token, err := oa.Config.Exchange(context.Background(), code)
	if err != nil {
		return "", err
	}
	if oa.UserInfoURL == "" {
		return "", errors.New("UserInfoURL is empty")
	}
	return token.AccessToken, nil
}

func (oa *baseOauth2) OauthLinkUser(u *oauth2User, db *gorm.DB) (*model.User, error) {
	if u.Name == "admin" {
		return nil, errors.New("admin is not allowed to login by oauth")
	}
	encryptedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte("Dragonfly2"), bcrypt.MinCost)
	if err != nil {
		return nil, err
	}
	var userCount int64
	if err := db.Model(model.User{}).Where("name = ?", u.Name).Count(&userCount).Error; err != nil {
		return nil, err
	}
	if userCount <= 0 {
		user := model.User{
			EncryptedPassword: string(encryptedPasswordBytes),
			Name:              u.Name,
			Email:             u.Email,
		}

		if err := db.Create(&user).Error; err != nil {
			return nil, err
		}
		return &user, nil

	}
	user := model.User{}
	if err := db.Model(model.User{}).Where("name = ?", u.Name).First(&user).Error; err != nil {
		return nil, err
	}
	return &user, nil

}
