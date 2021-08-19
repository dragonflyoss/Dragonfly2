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

type SignInRequest struct {
	Name     string `form:"name" binding:"required,min=3,max=10"`
	Password string `form:"password" binding:"required,min=8,max=20"`
}

type SignUpRequest struct {
	SignInRequest
	Email    string `form:"email" binding:"required,email"`
	Phone    string `form:"phone" binding:"omitempty"`
	Avatar   string `form:"avatar" binding:"omitempty"`
	Location string `form:"location" binding:"omitempty"`
	BIO      string `form:"bio" binding:"omitempty"`
}

type RoleRequest struct {
	RoleName string `uri:"role_name" binding:"required,min=1"`
	ID       uint   `uri:"id" binding:"required,min=1"`
}
