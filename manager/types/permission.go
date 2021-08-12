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

type PolicyRequest struct {
	Subject string `form:"subject" binding:"required,min=1"`
	Object  string `form:"object" binding:"required,min=1"`
	Action  string `form:"aciton" binding:"omitempty,oneof=read write"`
}

type PermissionGroups []string

type UserRolesParams struct {
	Subject string `uri:"subject" binding:"required"`
}

type UserHasRoleParams struct {
	UserRolesParams
	Object string `uri:"object" binding:"required"`
	Action string `uri:"action" binding:"omitempty,oneof=read write"`
}
