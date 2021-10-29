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

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/job"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"

	"gorm.io/gorm"
)

type REST interface {
	GetUser(context.Context, uint) (*model.User, error)
	GetUsers(context.Context, types.GetUsersQuery) (*[]model.User, error)
	UserTotalCount(context.Context, types.GetUsersQuery) (int64, error)
	SignIn(context.Context, types.SignInRequest) (*model.User, error)
	SignUp(context.Context, types.SignUpRequest) (*model.User, error)
	OauthSignin(context.Context, string) (string, error)
	OauthSigninCallback(context.Context, string, string) (*model.User, error)
	ResetPassword(context.Context, uint, types.ResetPasswordRequest) error
	GetRolesForUser(context.Context, uint) ([]string, error)
	AddRoleForUser(context.Context, types.AddRoleForUserParams) (bool, error)
	DeleteRoleForUser(context.Context, types.DeleteRoleForUserParams) (bool, error)

	CreateRole(context.Context, types.CreateRoleRequest) error
	DestroyRole(context.Context, string) (bool, error)
	GetRole(context.Context, string) [][]string
	GetRoles(context.Context) []string
	AddPermissionForRole(context.Context, string, types.AddPermissionForRoleRequest) (bool, error)
	DeletePermissionForRole(context.Context, string, types.DeletePermissionForRoleRequest) (bool, error)

	GetPermissions(context.Context, *gin.Engine) []rbac.Permission

	CreateOauth(context.Context, types.CreateOauthRequest) (*model.Oauth, error)
	DestroyOauth(context.Context, uint) error
	UpdateOauth(context.Context, uint, types.UpdateOauthRequest) (*model.Oauth, error)
	GetOauth(context.Context, uint) (*model.Oauth, error)
	GetOauths(context.Context, types.GetOauthsQuery) (*[]model.Oauth, error)
	OauthTotalCount(context.Context, types.GetOauthsQuery) (int64, error)

	CreateCDNCluster(context.Context, types.CreateCDNClusterRequest) (*model.CDNCluster, error)
	CreateCDNClusterWithSecurityGroupDomain(context.Context, types.CreateCDNClusterRequest) (*model.CDNCluster, error)
	DestroyCDNCluster(context.Context, uint) error
	UpdateCDNCluster(context.Context, uint, types.UpdateCDNClusterRequest) (*model.CDNCluster, error)
	UpdateCDNClusterWithSecurityGroupDomain(context.Context, uint, types.UpdateCDNClusterRequest) (*model.CDNCluster, error)
	GetCDNCluster(context.Context, uint) (*model.CDNCluster, error)
	GetCDNClusters(context.Context, types.GetCDNClustersQuery) (*[]model.CDNCluster, error)
	CDNClusterTotalCount(context.Context, types.GetCDNClustersQuery) (int64, error)
	AddCDNToCDNCluster(context.Context, uint, uint) error
	AddSchedulerClusterToCDNCluster(context.Context, uint, uint) error

	CreateCDN(context.Context, types.CreateCDNRequest) (*model.CDN, error)
	DestroyCDN(context.Context, uint) error
	UpdateCDN(context.Context, uint, types.UpdateCDNRequest) (*model.CDN, error)
	GetCDN(context.Context, uint) (*model.CDN, error)
	GetCDNs(context.Context, types.GetCDNsQuery) (*[]model.CDN, error)
	CDNTotalCount(context.Context, types.GetCDNsQuery) (int64, error)

	CreateSchedulerCluster(context.Context, types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	CreateSchedulerClusterWithSecurityGroupDomain(context.Context, types.CreateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	DestroySchedulerCluster(context.Context, uint) error
	UpdateSchedulerCluster(context.Context, uint, types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	UpdateSchedulerClusterWithSecurityGroupDomain(context.Context, uint, types.UpdateSchedulerClusterRequest) (*model.SchedulerCluster, error)
	GetSchedulerCluster(context.Context, uint) (*model.SchedulerCluster, error)
	GetSchedulerClusters(context.Context, types.GetSchedulerClustersQuery) (*[]model.SchedulerCluster, error)
	SchedulerClusterTotalCount(context.Context, types.GetSchedulerClustersQuery) (int64, error)
	AddSchedulerToSchedulerCluster(context.Context, uint, uint) error

	CreateScheduler(context.Context, types.CreateSchedulerRequest) (*model.Scheduler, error)
	DestroyScheduler(context.Context, uint) error
	UpdateScheduler(context.Context, uint, types.UpdateSchedulerRequest) (*model.Scheduler, error)
	GetScheduler(context.Context, uint) (*model.Scheduler, error)
	GetSchedulers(context.Context, types.GetSchedulersQuery) (*[]model.Scheduler, error)
	SchedulerTotalCount(context.Context, types.GetSchedulersQuery) (int64, error)

	CreateSecurityGroup(context.Context, types.CreateSecurityGroupRequest) (*model.SecurityGroup, error)
	DestroySecurityGroup(context.Context, uint) error
	UpdateSecurityGroup(context.Context, uint, types.UpdateSecurityGroupRequest) (*model.SecurityGroup, error)
	GetSecurityGroup(context.Context, uint) (*model.SecurityGroup, error)
	GetSecurityGroups(context.Context, types.GetSecurityGroupsQuery) (*[]model.SecurityGroup, error)
	SecurityGroupTotalCount(context.Context, types.GetSecurityGroupsQuery) (int64, error)
	AddSchedulerClusterToSecurityGroup(context.Context, uint, uint) error
	AddCDNClusterToSecurityGroup(context.Context, uint, uint) error

	CreateConfig(context.Context, types.CreateConfigRequest) (*model.Config, error)
	DestroyConfig(context.Context, uint) error
	UpdateConfig(context.Context, uint, types.UpdateConfigRequest) (*model.Config, error)
	GetConfig(context.Context, uint) (*model.Config, error)
	GetConfigs(context.Context, types.GetConfigsQuery) (*[]model.Config, error)
	ConfigTotalCount(context.Context, types.GetConfigsQuery) (int64, error)

	CreatePreheatJob(context.Context, types.CreatePreheatJobRequest) (*model.Job, error)
	DestroyJob(context.Context, uint) error
	UpdateJob(context.Context, uint, types.UpdateJobRequest) (*model.Job, error)
	GetJob(context.Context, uint) (*model.Job, error)
	GetJobs(context.Context, types.GetJobsQuery) (*[]model.Job, error)
	JobTotalCount(context.Context, types.GetJobsQuery) (int64, error)

	CreateV1Preheat(context.Context, types.CreateV1PreheatRequest) (*types.CreateV1PreheatResponse, error)
	GetV1Preheat(context.Context, string) (*types.GetV1PreheatResponse, error)
}

type rest struct {
	db       *gorm.DB
	rdb      *redis.Client
	cache    *cache.Cache
	job      *job.Job
	enforcer *casbin.Enforcer
}

// NewREST returns a new REST instence
func NewREST(database *database.Database, cache *cache.Cache, job *job.Job, enforcer *casbin.Enforcer) REST {
	return &rest{
		db:       database.DB,
		rdb:      database.RDB,
		cache:    cache,
		job:      job,
		enforcer: enforcer,
	}
}
