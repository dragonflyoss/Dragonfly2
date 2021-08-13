package service

import (
	"d7y.io/dragonfly/v2/manager/auth/oauth"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"golang.org/x/oauth2/github"
	"golang.org/x/oauth2/google"
)

func (s *rest) CreateOauth(json types.CreateOauthRequest) (*model.Oauth, error) {
	o := model.Oauth{}
	o.ClientID = json.ClientID
	o.ClientSecret = json.ClientSecret
	o.Name = json.Name
	switch json.Name {
	case oauth.Google:
		o.AuthURL = google.Endpoint.AuthURL
		o.TokenURL = google.Endpoint.TokenURL
		o.Scopes = oauth.GoogleScopes
		o.UserInfoURL = oauth.GoogleUserInfoURL

	case oauth.Github:
		o.AuthURL = github.Endpoint.AuthURL
		o.TokenURL = github.Endpoint.TokenURL
		o.Scopes = oauth.GithubScopes
		o.UserInfoURL = oauth.GithubUserInfoURL
	default:
		o = model.Oauth{
			ClientID:     json.ClientID,
			ClientSecret: json.ClientSecret,
			Name:         json.Name,
			Scopes:       json.Scopes,
			AuthURL:      json.AuthURL,
			TokenURL:     json.TokenURL,
			UserInfoURL:  json.UserInfoURL,
		}
	}

	if err := s.db.Create(&o).Error; err != nil {
		return nil, err
	}

	return &o, nil
}

func (s *rest) DestroyOauth(id uint) error {
	if err := s.db.Unscoped().Delete(&model.Oauth{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) UpdateOauth(id uint, json types.UpdateOauthRequest) (*model.Oauth, error) {
	oauth := model.Oauth{}
	if err := s.db.First(&oauth, id).Updates(model.Oauth{
		ClientID:     json.ClientID,
		ClientSecret: json.ClientSecret,
		AuthURL:      json.AuthURL,
		TokenURL:     json.TokenURL,
	}).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
}

func (s *rest) GetOauth(id uint) (*model.Oauth, error) {
	oauth := model.Oauth{}
	if err := s.db.First(&oauth, id).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
}

func (s *rest) GetOauths() (*[]model.Oauth, error) {
	oauths := []model.Oauth{}
	if err := s.db.Find(&oauths).Error; err != nil {
		return nil, err
	}
	return &oauths, nil
}

func (s *rest) OauthSignin(name string) (string, error) {
	oauthModel := model.Oauth{}
	if err := s.db.First(&oauthModel, name).Error; err != nil {
		return "", err
	}

	o, err := oauth.New(&oauthModel, s.db)
	if err != nil {
		return "", err
	}
	return o.AuthCodeURL(), nil
}

func (s *rest) OauthCallback(name, code string) (*model.User, error) {
	oauthModel := model.Oauth{}
	if err := s.db.First(&oauthModel, name).Error; err != nil {
		return nil, err
	}
	o, err := oauth.New(&oauthModel, s.db)
	if err != nil {
		return nil, err
	}

	user, err := o.ExchangeUserByCode(code, s.db)
	if err != nil {
		return nil, err
	}
	return user, nil
}
