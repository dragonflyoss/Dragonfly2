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
	"context"
	"errors"
	"fmt"

	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"

	manageroauth "d7y.io/dragonfly/v2/manager/auth/oauth"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
)

func (s *service) UpdateUser(ctx context.Context, id uint, json types.UpdateUserRequest) (*models.User, error) {
	user := models.User{}
	if err := s.db.WithContext(ctx).First(&user, id).Updates(models.User{
		Email:    json.Email,
		Phone:    json.Phone,
		Avatar:   json.Avatar,
		Location: json.Location,
		BIO:      json.BIO,
	}).Error; err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *service) GetUser(ctx context.Context, id uint) (*models.User, error) {
	user := models.User{}
	if err := s.db.WithContext(ctx).First(&user, id).Error; err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *service) GetUsers(ctx context.Context, q types.GetUsersQuery) ([]models.User, int64, error) {
	var count int64
	var users []models.User
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.User{
		Name:     q.Name,
		Email:    q.Email,
		Location: q.Location,
		State:    q.State,
	}).Find(&users).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return users, count, nil
}

func (s *service) SignIn(ctx context.Context, json types.SignInRequest) (*models.User, error) {
	user := models.User{}
	if err := s.db.WithContext(ctx).First(&user, models.User{
		Name: json.Name,
	}).Error; err != nil {
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.EncryptedPassword), []byte(json.Password)); err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *service) ResetPassword(ctx context.Context, id uint, json types.ResetPasswordRequest) error {
	user := models.User{}
	if err := s.db.WithContext(ctx).First(&user, id).Error; err != nil {
		return err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.EncryptedPassword), []byte(json.OldPassword)); err != nil {
		return err
	}

	encryptedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte(json.NewPassword), bcrypt.MinCost)
	if err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).First(&user, id).Updates(models.User{
		EncryptedPassword: string(encryptedPasswordBytes),
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) SignUp(ctx context.Context, json types.SignUpRequest) (*models.User, error) {
	encryptedPasswordBytes, err := bcrypt.GenerateFromPassword([]byte(json.Password), bcrypt.MinCost)
	if err != nil {
		return nil, err
	}

	user := models.User{
		EncryptedPassword: string(encryptedPasswordBytes),
		Name:              json.Name,
		Email:             json.Email,
		Phone:             json.Phone,
		Avatar:            json.Avatar,
		Location:          json.Location,
		BIO:               json.BIO,
		State:             models.UserStateEnabled,
	}

	if err := s.db.WithContext(ctx).Create(&user).Error; err != nil {
		return nil, err
	}

	if _, err := s.enforcer.AddRoleForUser(fmt.Sprint(user.ID), rbac.GuestRole); err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *service) OauthSignin(ctx context.Context, name string) (string, error) {
	oauth := models.Oauth{}
	if err := s.db.WithContext(ctx).First(&oauth, models.Oauth{Name: name}).Error; err != nil {
		return "", err
	}

	o, err := manageroauth.New(oauth.Name, oauth.ClientID, oauth.ClientSecret, oauth.RedirectURL)
	if err != nil {
		return "", err
	}

	return o.AuthCodeURL()
}

func (s *service) OauthSigninCallback(ctx context.Context, name, code string) (*models.User, error) {
	oauth := models.Oauth{}
	if err := s.db.WithContext(ctx).First(&oauth, models.Oauth{Name: name}).Error; err != nil {
		return nil, err
	}

	o, err := manageroauth.New(oauth.Name, oauth.ClientID, oauth.ClientSecret, oauth.RedirectURL)
	if err != nil {
		return nil, err
	}

	token, err := o.Exchange(code)
	if err != nil {
		return nil, err
	}

	oauthUser, err := o.GetUser(token)
	if err != nil {
		return nil, err
	}

	user := models.User{
		Name:   oauthUser.Name,
		Email:  oauthUser.Email,
		Avatar: oauthUser.Avatar,
		State:  models.UserStateEnabled,
	}
	if err := s.db.WithContext(ctx).Create(&user).Error; err != nil {
		var merr *mysql.MySQLError
		if errors.As(err, &merr) && merr.Number == mysqlerr.ER_DUP_ENTRY {
			return &user, nil
		}

		return nil, err
	}

	if _, err := s.enforcer.AddRoleForUser(fmt.Sprint(user.ID), rbac.GuestRole); err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *service) GetRolesForUser(ctx context.Context, id uint) ([]string, error) {
	return s.enforcer.GetRolesForUser(fmt.Sprint(id))
}

func (s *service) AddRoleForUser(ctx context.Context, json types.AddRoleForUserParams) (bool, error) {
	return s.enforcer.AddRoleForUser(fmt.Sprint(json.ID), json.Role)
}

func (s *service) DeleteRoleForUser(ctx context.Context, json types.DeleteRoleForUserParams) (bool, error) {
	return s.enforcer.DeleteRoleForUser(fmt.Sprint(json.ID), json.Role)
}
