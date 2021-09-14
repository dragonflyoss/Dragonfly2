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

import (
	"d7y.io/dragonfly/v2/manager/permission/rbac"
)

type CreateRoleRequest struct {
	Role        string            `json:"role" binding:"required"`
	Permissions []rbac.Permission `json:"permissions" binding:"required"`
}

type RoleParams struct {
	Role string `uri:"role" binding:"required"`
}

type AddPermissionForRoleRequest struct {
	rbac.Permission `json:",inline" binding:"required"`
}

type DeletePermissionForRoleRequest struct {
	rbac.Permission `json:",inline" binding:"required"`
}
