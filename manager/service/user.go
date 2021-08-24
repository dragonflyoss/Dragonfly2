/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package service

import (
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"golang.org/x/crypto/bcrypt"
)

func (s *rest) SignIn(json types.SignInRequest) (*model.User, error) {
	user := model.User{}
	if err := s.db.First(&user, model.User{
		Name: json.Name,
	}).Error; err != nil {
		return nil, err
	}

	err := bcrypt.CompareHashAndPassword([]byte(user.EncryptedPassword), []byte(json.Password))
	if err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *rest) SignUp(json types.SignUpRequest) (*model.User, error) {
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
		BIO:               json.BIO,
		State:             model.UserStateEnabled,
	}

	if err := s.db.Create(&user).Error; err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *rest) AddRoleForUser(UserID uint, roleName string) error {
	user := model.User{}
	if err := s.db.First(&user, UserID).Error; err != nil {
		return err
	}
	_, err := s.enforcer.AddRoleForUser(user.Name, roleName)
	if err != nil {
		return err
	}
	return nil
}

func (s *rest) DeleteRoleForUser(UserID uint, roleName string) error {
	user := model.User{}
	if err := s.db.First(&user, UserID).Error; err != nil {
		return err
	}
	_, err := s.enforcer.DeleteRoleForUser(user.Name, roleName)
	if err != nil {
		return err
	}
	return nil
}
