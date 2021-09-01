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

type Oauth2Params struct {
	ID uint `uri:"id" binding:"required"`
}

type CreateOauth2Request struct {
	Name         string `json:"name" binding:"required,oneof=github google"`
	BIO          string `json:"bio" binding:"omitempty"`
	ClientID     string `json:"client_id" binding:"required"`
	ClientSecret string `json:"client_secret" binding:"required"`
}

type UpdateOauth2Request struct {
	Name         string `json:"name" binding:"omitempty,oneof=github google"`
	BIO          string `json:"bio" binding:"omitempty"`
	ClientID     string `json:"client_id" binding:"omitempty"`
	ClientSecret string `json:"client_secret" binding:"omitempty"`
}

type GetOauth2sQuery struct {
	Page     int    `form:"page" binding:"omitempty,gte=1"`
	PerPage  int    `form:"per_page" binding:"omitempty,gte=1,lte=50"`
	Name     string `json:"name" binding:"omitempty,oneof=github google"`
	ClientID string `json:"client_id" binding:"omitempty"`
}
