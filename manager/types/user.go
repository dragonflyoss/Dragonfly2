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

package types

type UpdateUserRequest struct {
	Email    string `json:"email" binding:"omitempty"`
	Phone    string `json:"phone" binding:"omitempty"`
	Avatar   string `json:"avatar" binding:"omitempty"`
	Location string `json:"location" binding:"omitempty"`
	BIO      string `json:"bio" binding:"omitempty"`
}

type UserParams struct {
	ID uint `uri:"id" binding:"required"`
}

type GetUsersQuery struct {
	Name     string `form:"name" binding:"omitempty"`
	Email    string `form:"email" binding:"omitempty"`
	Location string `form:"location" binding:"omitempty"`
	State    string `form:"state" binding:"omitempty"`
	Page     int    `form:"page" binding:"omitempty,gte=1"`
	PerPage  int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}

type SignInRequest struct {
	Name     string `json:"name" binding:"required,min=3,max=10"`
	Password string `json:"password" binding:"required,min=8,max=20"`
}

type OauthSigninParams struct {
	Name string `uri:"name" binding:"required"`
}

type OauthSigninCallbackParams struct {
	Name string `uri:"name" binding:"required"`
}

type OauthSigninCallbackQuery struct {
	Code string `form:"code" binding:"required"`
}

type ResetPasswordRequest struct {
	OldPassword string `json:"old_password" binding:"required,min=8,max=20"`
	NewPassword string `json:"new_password" binding:"required,min=8,max=20"`
}

type SignUpRequest struct {
	SignInRequest
	Email    string `json:"email" binding:"required,email"`
	Phone    string `json:"phone" binding:"omitempty"`
	Avatar   string `json:"avatar" binding:"omitempty"`
	Location string `json:"location" binding:"omitempty"`
	BIO      string `json:"bio" binding:"omitempty"`
}

type DeleteRoleForUserParams struct {
	ID   uint   `uri:"id" binding:"required"`
	Role string `uri:"role" binding:"required"`
}

type AddRoleForUserParams struct {
	ID   uint   `uri:"id" binding:"required"`
	Role string `uri:"role" binding:"required"`
}
