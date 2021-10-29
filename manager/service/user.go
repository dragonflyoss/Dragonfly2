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
	"fmt"

	"github.com/pkg/errors"

	manageroauth "d7y.io/dragonfly/v2/manager/auth/oauth"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/VividCortex/mysqlerr"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
)

func (s *rest) GetUser(ctx context.Context, id uint) (*model.User, error) {
	user := model.User{}
	if err := s.db.WithContext(ctx).First(&user, id).Error; err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *rest) GetUsers(ctx context.Context, q types.GetUsersQuery) (*[]model.User, error) {
	users := []model.User{}
	if err := s.db.WithContext(ctx).Scopes(model.Paginate(q.Page, q.PerPage)).Where(&model.User{
		Name:     q.Name,
		Email:    q.Email,
		Location: q.Location,
		State:    q.State,
	}).Find(&users).Error; err != nil {
		return nil, err
	}

	return &users, nil
}

func (s *rest) UserTotalCount(ctx context.Context, q types.GetUsersQuery) (int64, error) {
	var count int64
	if err := s.db.WithContext(ctx).Model(&model.User{}).Where(&model.User{
		Name:     q.Name,
		Email:    q.Email,
		Location: q.Location,
		State:    q.State,
	}).Count(&count).Error; err != nil {
		return 0, err
	}

	return count, nil
}

func (s *rest) SignIn(ctx context.Context, json types.SignInRequest) (*model.User, error) {
	user := model.User{}
	if err := s.db.WithContext(ctx).First(&user, model.User{
		Name: json.Name,
	}).Error; err != nil {
		return nil, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.EncryptedPassword), []byte(json.Password)); err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *rest) ResetPassword(ctx context.Context, id uint, json types.ResetPasswordRequest) error {
	user := model.User{}
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

	if err := s.db.WithContext(ctx).First(&user, id).Updates(model.User{
		EncryptedPassword: string(encryptedPasswordBytes),
	}).Error; err != nil {
		return err
	}

	return nil
}

func (s *rest) SignUp(ctx context.Context, json types.SignUpRequest) (*model.User, error) {
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

	if err := s.db.WithContext(ctx).Create(&user).Error; err != nil {
		return nil, err
	}

	if _, err := s.enforcer.AddRoleForUser(fmt.Sprint(user.ID), rbac.GuestRole); err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *rest) OauthSignin(ctx context.Context, name string) (string, error) {
	oauth := model.Oauth{}
	if err := s.db.WithContext(ctx).First(&oauth, model.Oauth{Name: name}).Error; err != nil {
		return "", err
	}

	o, err := manageroauth.New(oauth.Name, oauth.ClientID, oauth.ClientSecret, oauth.RedirectURL)
	if err != nil {
		return "", err
	}

	return o.AuthCodeURL(), nil
}

func (s *rest) OauthSigninCallback(ctx context.Context, name, code string) (*model.User, error) {
	oauth := model.Oauth{}
	if err := s.db.WithContext(ctx).First(&oauth, model.Oauth{Name: name}).Error; err != nil {
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

	user := model.User{
		Name:   oauthUser.Name,
		Email:  oauthUser.Email,
		Avatar: oauthUser.Avatar,
		State:  model.UserStateEnabled,
	}
	if err := s.db.WithContext(ctx).Create(&user).Error; err != nil {
		if err, ok := errors.Cause(err).(*mysql.MySQLError); ok && err.Number == mysqlerr.ER_DUP_ENTRY {
			return &user, nil
		}

		return nil, err
	}

	if _, err := s.enforcer.AddRoleForUser(fmt.Sprint(user.ID), rbac.GuestRole); err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *rest) GetRolesForUser(ctx context.Context, id uint) ([]string, error) {
	return s.enforcer.GetRolesForUser(fmt.Sprint(id))
}

func (s *rest) AddRoleForUser(ctx context.Context, json types.AddRoleForUserParams) (bool, error) {
	return s.enforcer.AddRoleForUser(fmt.Sprint(json.ID), json.Role)
}

func (s *rest) DeleteRoleForUser(ctx context.Context, json types.DeleteRoleForUserParams) (bool, error) {
	return s.enforcer.DeleteRoleForUser(fmt.Sprint(json.ID), json.Role)
}
