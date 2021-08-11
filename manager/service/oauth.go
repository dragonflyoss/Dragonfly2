package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/oauth"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *rest) CreateOauth(json types.CreateOauthRequest) (*model.Oauth, error) {
	oauth := model.Oauth{
		ClientID:     json.ClientID,
		ClientSecret: json.ClientSecret,
		Name:         json.Name,
		AuthURL:      json.AuthURL,
		TokenURL:     json.TokenURL,
	}

	if err := s.db.Create(&oauth).Error; err != nil {
		return nil, err
	}

	return &oauth, nil
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

func (s *rest) GetOauths(q types.GetOauthsQuery) (*[]model.Oauth, error) {
	oauths := []model.Oauth{}
	if q.Name != "" {
		if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.Oauth{
			Name: q.Name,
		}).Find(&oauths).Error; err != nil {
			return nil, err
		}

	} else {
		if err := s.db.Scopes(model.Paginate(q.Page, q.PerPage)).Find(&oauths).Error; err != nil {
			return nil, err
		}
	}
	return &oauths, nil
}

func (s *rest) OauthTotalCount(q types.GetOauthsQuery) (int64, error) {
	var count int64
	if q.Name != "" {
		if err := s.db.Model(&model.Oauth{}).Where(&model.Oauth{
			Name: q.Name,
		}).Count(&count).Error; err != nil {
			return 0, err
		}

	} else {
		if err := s.db.Model(&model.Oauth{}).Where(&model.Oauth{}).Count(&count).Error; err != nil {
			return 0, err
		}

	}

	return count, nil
}

func (s *rest) OauthSignin(name string) (string, error) {
	oauthModel := model.Oauth{}
	if err := s.db.First(&oauthModel, name).Error; err != nil {
		return "", err
	}

	o, err := oauth.NewBaseOauth2(name, oauthModel.ClientID, oauthModel.ClientSecret, oauthModel.Scopes, oauthModel.AuthURL, oauthModel.TokenURL, s.db)
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

	o, err := oauth.NewBaseOauth2(name, oauthModel.ClientID, oauthModel.ClientSecret, oauthModel.Scopes, oauthModel.AuthURL, oauthModel.TokenURL, s.db)
	if err != nil {
		return nil, err
	}
	user, err := o.ExchangeUserByCode(code, s.db)
	if err != nil {
		return nil, err
	}
	return user, nil
}
