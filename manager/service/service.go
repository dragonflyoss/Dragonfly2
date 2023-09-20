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

//go:generate mockgen -destination mocks/service_mock.go -source service.go -package mocks

package service

import (
	"context"

	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"

	"d7y.io/dragonfly/v2/manager/cache"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/job"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
)

type Service interface {
	UpdateUser(context.Context, uint, types.UpdateUserRequest) (*models.User, error)
	GetUser(context.Context, uint) (*models.User, error)
	GetUsers(context.Context, types.GetUsersQuery) ([]models.User, int64, error)
	SignIn(context.Context, types.SignInRequest) (*models.User, error)
	SignUp(context.Context, types.SignUpRequest) (*models.User, error)
	OauthSignin(context.Context, string) (string, error)
	OauthSigninCallback(context.Context, string, string) (*models.User, error)
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

	CreateOauth(context.Context, types.CreateOauthRequest) (*models.Oauth, error)
	DestroyOauth(context.Context, uint) error
	UpdateOauth(context.Context, uint, types.UpdateOauthRequest) (*models.Oauth, error)
	GetOauth(context.Context, uint) (*models.Oauth, error)
	GetOauths(context.Context, types.GetOauthsQuery) ([]models.Oauth, int64, error)

	CreateCluster(context.Context, types.CreateClusterRequest) (*types.CreateClusterResponse, error)
	DestroyCluster(context.Context, uint) error
	UpdateCluster(context.Context, uint, types.UpdateClusterRequest) (*types.UpdateClusterResponse, error)
	GetCluster(context.Context, uint) (*types.GetClusterResponse, error)
	GetClusters(context.Context, types.GetClustersQuery) ([]types.GetClusterResponse, int64, error)

	CreateSeedPeerCluster(context.Context, types.CreateSeedPeerClusterRequest) (*models.SeedPeerCluster, error)
	DestroySeedPeerCluster(context.Context, uint) error
	UpdateSeedPeerCluster(context.Context, uint, types.UpdateSeedPeerClusterRequest) (*models.SeedPeerCluster, error)
	GetSeedPeerCluster(context.Context, uint) (*models.SeedPeerCluster, error)
	GetSeedPeerClusters(context.Context, types.GetSeedPeerClustersQuery) ([]models.SeedPeerCluster, int64, error)
	AddSeedPeerToSeedPeerCluster(context.Context, uint, uint) error
	AddSchedulerClusterToSeedPeerCluster(context.Context, uint, uint) error

	CreateSeedPeer(context.Context, types.CreateSeedPeerRequest) (*models.SeedPeer, error)
	DestroySeedPeer(context.Context, uint) error
	UpdateSeedPeer(context.Context, uint, types.UpdateSeedPeerRequest) (*models.SeedPeer, error)
	GetSeedPeer(context.Context, uint) (*models.SeedPeer, error)
	GetSeedPeers(context.Context, types.GetSeedPeersQuery) ([]models.SeedPeer, int64, error)

	CreatePeer(context.Context, types.CreatePeerRequest) (*models.Peer, error)
	DestroyPeer(context.Context, uint) error
	GetPeer(context.Context, uint) (*models.Peer, error)
	GetPeers(context.Context, types.GetPeersQuery) ([]models.Peer, int64, error)

	CreateSchedulerCluster(context.Context, types.CreateSchedulerClusterRequest) (*models.SchedulerCluster, error)
	DestroySchedulerCluster(context.Context, uint) error
	UpdateSchedulerCluster(context.Context, uint, types.UpdateSchedulerClusterRequest) (*models.SchedulerCluster, error)
	GetSchedulerCluster(context.Context, uint) (*models.SchedulerCluster, error)
	GetSchedulerClusters(context.Context, types.GetSchedulerClustersQuery) ([]models.SchedulerCluster, int64, error)
	AddSchedulerToSchedulerCluster(context.Context, uint, uint) error

	CreateScheduler(context.Context, types.CreateSchedulerRequest) (*models.Scheduler, error)
	DestroyScheduler(context.Context, uint) error
	UpdateScheduler(context.Context, uint, types.UpdateSchedulerRequest) (*models.Scheduler, error)
	GetScheduler(context.Context, uint) (*models.Scheduler, error)
	GetSchedulers(context.Context, types.GetSchedulersQuery) ([]models.Scheduler, int64, error)

	CreateBucket(context.Context, types.CreateBucketRequest) error
	DestroyBucket(context.Context, string) error
	GetBucket(context.Context, string) (*objectstorage.BucketMetadata, error)
	GetBuckets(context.Context) ([]*objectstorage.BucketMetadata, error)

	CreateConfig(context.Context, types.CreateConfigRequest) (*models.Config, error)
	DestroyConfig(context.Context, uint) error
	UpdateConfig(context.Context, uint, types.UpdateConfigRequest) (*models.Config, error)
	GetConfig(context.Context, uint) (*models.Config, error)
	GetConfigs(context.Context, types.GetConfigsQuery) ([]models.Config, int64, error)

	CreatePreheatJob(context.Context, types.CreatePreheatJobRequest) (*models.Job, error)
	DestroyJob(context.Context, uint) error
	UpdateJob(context.Context, uint, types.UpdateJobRequest) (*models.Job, error)
	GetJob(context.Context, uint) (*models.Job, error)
	GetJobs(context.Context, types.GetJobsQuery) ([]models.Job, int64, error)

	CreateV1Preheat(context.Context, types.CreateV1PreheatRequest) (*types.CreateV1PreheatResponse, error)
	GetV1Preheat(context.Context, string) (*types.GetV1PreheatResponse, error)

	CreateApplication(context.Context, types.CreateApplicationRequest) (*models.Application, error)
	DestroyApplication(context.Context, uint) error
	UpdateApplication(context.Context, uint, types.UpdateApplicationRequest) (*models.Application, error)
	GetApplication(context.Context, uint) (*models.Application, error)
	GetApplications(context.Context, types.GetApplicationsQuery) ([]models.Application, int64, error)

	DestroyModel(context.Context, uint) error
	UpdateModel(context.Context, uint, types.UpdateModelRequest) (*models.Model, error)
	GetModel(context.Context, uint) (*models.Model, error)
	GetModels(context.Context, types.GetModelsQuery) ([]models.Model, int64, error)

	CreatePersonalAccessToken(context.Context, types.CreatePersonalAccessTokenRequest) (*models.PersonalAccessToken, error)
	DestroyPersonalAccessToken(context.Context, uint) error
	UpdatePersonalAccessToken(context.Context, uint, types.UpdatePersonalAccessTokenRequest) (*models.PersonalAccessToken, error)
	GetPersonalAccessToken(context.Context, uint) (*models.PersonalAccessToken, error)
	GetPersonalAccessTokens(context.Context, types.GetPersonalAccessTokensQuery) ([]models.PersonalAccessToken, int64, error)
}

type service struct {
	config        *config.Config
	db            *gorm.DB
	rdb           redis.UniversalClient
	cache         *cache.Cache
	job           *job.Job
	enforcer      *casbin.Enforcer
	objectStorage objectstorage.ObjectStorage
}

// NewREST returns a new REST instence
func New(cfg *config.Config, database *database.Database, cache *cache.Cache, job *job.Job, enforcer *casbin.Enforcer, objectStorage objectstorage.ObjectStorage) Service {
	return &service{
		config:        cfg,
		db:            database.DB,
		rdb:           database.RDB,
		cache:         cache,
		job:           job,
		enforcer:      enforcer,
		objectStorage: objectStorage,
	}
}
