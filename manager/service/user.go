package service

import (
	"errors"

	"d7y.io/dragonfly.v2/manager/model"
	"d7y.io/dragonfly.v2/manager/types"
	"golang.org/x/crypto/bcrypt"
)

func (s *rest) Login(json types.LoginRequest) (*model.User, error) {
	user := model.User{}
	if err := s.db.Where("name = ?", json.Name).First(&user).Error; err != nil {
		return nil, err
	}
	err := bcrypt.CompareHashAndPassword([]byte(user.EncryptedPassword), []byte(json.Password))
	if err != nil {
		return nil, errors.New("password mismatch")
	}

	return &user, nil

}

func (s *rest) Register(json types.RegisterRequest) (*model.User, error) {

	encryptedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte(json.Password), bcrypt.MinCost)
	if err != nil {
		return nil, err
	}
	user := model.User{
		EncryptedPassword: string(encryptedPasswordBytes),
		Name:              json.Name,
		Email:             json.Email,
		Phone:             json.Phone,
		Avatar:            json.Avatar,
		Location:          json.Location,
		Bio:               json.Bio,
		State:             model.UserStateEnabled,
	}

	if err := s.db.Create(&user).Error; err != nil {
		return nil, err
	}

	return &user, nil

}
