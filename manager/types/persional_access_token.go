/*
 *     Copyright 2023 The Dragonfly Authors
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

import "time"

const (
	// PersonalAccessTokenScopePreheat represents the personal access token whose scope is preheat.
	PersonalAccessTokenScopePreheat = "preheat"

	// PersonalAccessTokenScopeJob represents the personal access token whose scope is job.
	PersonalAccessTokenScopeJob = "job"

	// PersonalAccessTokenScopeCluster represents the personal access token whose scope is cluster.
	PersonalAccessTokenScopeCluster = "cluster"
)

type CreatePersonalAccessTokenRequest struct {
	Name      string    `json:"name" binding:"required"`
	BIO       string    `json:"bio" binding:"omitempty"`
	Scopes    []string  `json:"scopes" binding:"omitempty"`
	ExpiredAt time.Time `json:"expired_at" binding:"required"`
	UserID    uint      `json:"user_id" binding:"required"`
}

type UpdatePersonalAccessTokenRequest struct {
	BIO       string    `json:"bio" binding:"omitempty"`
	Scopes    []string  `json:"scopes" binding:"omitempty"`
	State     string    `json:"state" binding:"omitempty,oneof=active inactive"`
	ExpiredAt time.Time `json:"expired_at" binding:"omitempty"`
	UserID    uint      `json:"user_id" binding:"omitempty"`
}

type PersonalAccessTokenParams struct {
	ID uint `uri:"id" binding:"required"`
}

type GetPersonalAccessTokensQuery struct {
	State   string `form:"state" binding:"omitempty,oneof=active inactive"`
	UserID  uint   `form:"user_id" binding:"omitempty"`
	Page    int    `form:"page" binding:"omitempty,gte=1"`
	PerPage int    `form:"per_page" binding:"omitempty,gte=1,lte=10000000"`
}
