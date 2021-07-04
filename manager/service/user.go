package service

import (
	"errors"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/digestutils"
)

const defaultAvatar = "http://defaultAvatar"

func (s *rest) Login(json types.LoginRequest) (*model.User, error) {
	user := model.User{}
	if err := s.db.Where("name = ?", json.Name).First(&user).Error; err != nil {
		return nil, err
	}
	if digestutils.Sha256(json.Password, user.PasswordSalt) != user.EncryptedPassword {
		return nil, errors.New("password mismatch")
	}

	return &user, nil

}

func (s *rest) Register(json types.RegisterRequest) (*model.User, error) {
	passwordSalt, err := digestutils.GenerateRandomSalt(16)
	if err != nil {
		return nil, err
	}

	avatar := ""
	if len(json.Avatar) == 0 {
		avatar = defaultAvatar
	} else {
		avatar = json.Avatar
	}

	encryptedPassword := digestutils.Sha256(json.Password, passwordSalt)
	user := model.User{
		EncryptedPassword: encryptedPassword,
		Name:              json.Name,
		Email:             json.Email,
		PasswordSalt:      passwordSalt,
		Phone:             json.Phone,
		Avatar:            avatar,
		Location:          json.Location,
		Bio:               json.Bio,
		State:             model.UserStateEnabled,
	}

	if err := s.db.Create(&user).Error; err != nil {
		return nil, err
	}

	return &user, nil

}
