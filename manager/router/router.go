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

package router

import (
	"net/http"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/static"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/database"
	"d7y.io/dragonfly/v2/manager/handlers"
	"d7y.io/dragonfly/v2/manager/middlewares"
	"d7y.io/dragonfly/v2/manager/service"
)

const (
	PrometheusSubsystemName = "dragonfly_manager"
	OtelServiceName         = "dragonfly-manager"
)

func Init(cfg *config.Config, logDir string, service service.Service, database *database.Database, enforcer *casbin.Enforcer, assets static.ServeFileSystem) (*gin.Engine, error) {
	// Set mode.
	if !cfg.Verbose {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	h := handlers.New(service)

	// Prometheus metrics.
	p := ginprometheus.NewPrometheus(PrometheusSubsystemName)
	// URL removes query string.
	// Prometheus metrics need to reduce label,
	// refer to https://prometheus.io/docs/practices/instrumentation/#do-not-overuse-labels.
	p.ReqCntURLLabelMappingFn = func(c *gin.Context) string {
		return c.Request.URL.Path
	}
	p.Use(r)

	// Opentelemetry.
	if cfg.Options.Telemetry.Jaeger != "" {
		r.Use(otelgin.Middleware(OtelServiceName))
	}

	// Gin middleware.
	r.Use(gin.Recovery())
	r.Use(ginzap.Ginzap(logger.GinLogger.Desugar(), time.RFC3339, true))
	r.Use(ginzap.RecoveryWithZap(logger.GinLogger.Desugar(), true))

	// CORS middleware.
	r.Use(middlewares.CORS())

	// gzip middleware.
	r.Use(gzip.Gzip(gzip.DefaultCompression, gzip.WithExcludedExtensions([]string{".js", ".css"})))

	// RBAC middleware.
	rbac := middlewares.RBAC(enforcer)
	jwt, err := middlewares.Jwt(cfg.Auth.JWT, service)
	if err != nil {
		return nil, err
	}

	// Personal access token middleware.
	personalAccessToken := middlewares.PersonalAccessToken(database.DB)

	// Error middleware.
	r.Use(middlewares.Error())

	// Manager view.
	r.Use(static.Serve("/", assets))

	// API router.
	apiv1 := r.Group("/api/v1")

	// User.
	u := apiv1.Group("/users")
	u.PATCH(":id", jwt.MiddlewareFunc(), rbac, h.UpdateUser)
	u.GET(":id", jwt.MiddlewareFunc(), rbac, h.GetUser)
	u.GET("", jwt.MiddlewareFunc(), rbac, h.GetUsers)
	u.POST("signin", jwt.LoginHandler)
	u.POST("signout", jwt.LogoutHandler)
	u.POST("signup", h.SignUp)
	u.GET("signin/:name", h.OauthSignin)
	u.GET("signin/:name/callback", h.OauthSigninCallback(jwt))
	u.POST("refresh_token", jwt.RefreshHandler)
	u.POST(":id/reset_password", h.ResetPassword)
	u.GET(":id/roles", jwt.MiddlewareFunc(), rbac, h.GetRolesForUser)
	u.PUT(":id/roles/:role", jwt.MiddlewareFunc(), rbac, h.AddRoleToUser)
	u.DELETE(":id/roles/:role", jwt.MiddlewareFunc(), rbac, h.DeleteRoleForUser)

	// Role.
	re := apiv1.Group("/roles", jwt.MiddlewareFunc(), rbac)
	re.POST("", h.CreateRole)
	re.DELETE(":role", h.DestroyRole)
	re.GET(":role", h.GetRole)
	re.GET("", h.GetRoles)
	re.POST(":role/permissions", h.AddPermissionForRole)
	re.DELETE(":role/permissions", h.DeletePermissionForRole)

	// Permission.
	pm := apiv1.Group("/permissions", jwt.MiddlewareFunc(), rbac)
	pm.GET("", h.GetPermissions(r))

	// Oauth.
	oa := apiv1.Group("/oauth")
	oa.POST("", jwt.MiddlewareFunc(), rbac, h.CreateOauth)
	oa.DELETE(":id", jwt.MiddlewareFunc(), rbac, h.DestroyOauth)
	oa.PATCH(":id", jwt.MiddlewareFunc(), rbac, h.UpdateOauth)
	oa.GET(":id", h.GetOauth)
	oa.GET("", h.GetOauths)

	// Cluster.
	c := apiv1.Group("/clusters", jwt.MiddlewareFunc(), rbac)
	c.POST("", h.CreateCluster)
	c.DELETE(":id", h.DestroyCluster)
	c.PATCH(":id", h.UpdateCluster)
	c.GET(":id", h.GetCluster)
	c.GET("", h.GetClusters)

	// Scheduler Cluster.
	sc := apiv1.Group("/scheduler-clusters", jwt.MiddlewareFunc(), rbac)
	sc.POST("", h.CreateSchedulerCluster)
	sc.DELETE(":id", h.DestroySchedulerCluster)
	sc.PATCH(":id", h.UpdateSchedulerCluster)
	sc.GET(":id", h.GetSchedulerCluster)
	sc.GET("", h.GetSchedulerClusters)
	sc.PUT(":id/schedulers/:scheduler_id", h.AddSchedulerToSchedulerCluster)

	// Scheduler.
	s := apiv1.Group("/schedulers", jwt.MiddlewareFunc(), rbac)
	s.POST("", h.CreateScheduler)
	s.DELETE(":id", h.DestroyScheduler)
	s.PATCH(":id", h.UpdateScheduler)
	s.GET(":id", h.GetScheduler)
	s.GET("", h.GetSchedulers)

	// Seed Peer Cluster.
	spc := apiv1.Group("/seed-peer-clusters", jwt.MiddlewareFunc(), rbac)
	spc.POST("", h.CreateSeedPeerCluster)
	spc.DELETE(":id", h.DestroySeedPeerCluster)
	spc.PATCH(":id", h.UpdateSeedPeerCluster)
	spc.GET(":id", h.GetSeedPeerCluster)
	spc.GET("", h.GetSeedPeerClusters)
	spc.PUT(":id/seed-peers/:seed_peer_id", h.AddSeedPeerToSeedPeerCluster)
	spc.PUT(":id/scheduler-clusters/:scheduler_cluster_id", h.AddSchedulerClusterToSeedPeerCluster)

	// Seed Peer.
	sp := apiv1.Group("/seed-peers", jwt.MiddlewareFunc(), rbac)
	sp.POST("", h.CreateSeedPeer)
	sp.DELETE(":id", h.DestroySeedPeer)
	sp.PATCH(":id", h.UpdateSeedPeer)
	sp.GET(":id", h.GetSeedPeer)
	sp.GET("", h.GetSeedPeers)

	// Peer.
	peer := apiv1.Group("/peers", jwt.MiddlewareFunc(), rbac)
	peer.POST("", h.CreatePeer)
	peer.DELETE(":id", h.DestroyPeer)
	peer.GET(":id", h.GetPeer)
	peer.GET("", h.GetPeers)

	// Bucket.
	bucket := apiv1.Group("/buckets", jwt.MiddlewareFunc(), rbac)
	bucket.POST("", h.CreateBucket)
	bucket.DELETE(":id", h.DestroyBucket)
	bucket.GET(":id", h.GetBucket)
	bucket.GET("", h.GetBuckets)

	// Config.
	config := apiv1.Group("/configs")
	config.POST("", jwt.MiddlewareFunc(), rbac, h.CreateConfig)
	config.DELETE(":id", jwt.MiddlewareFunc(), rbac, h.DestroyConfig)
	config.PATCH(":id", jwt.MiddlewareFunc(), rbac, h.UpdateConfig)
	config.GET(":id", jwt.MiddlewareFunc(), rbac, h.GetConfig)
	config.GET("", h.GetConfigs)

	// TODO Add auth to the following routes and fix the tests.
	// Job.
	job := apiv1.Group("/jobs")
	job.POST("", h.CreateJob)
	job.DELETE(":id", h.DestroyJob)
	job.PATCH(":id", h.UpdateJob)
	job.GET(":id", h.GetJob)
	job.GET("", h.GetJobs)

	// Application.
	cs := apiv1.Group("/applications", jwt.MiddlewareFunc(), rbac)
	cs.POST("", h.CreateApplication)
	cs.DELETE(":id", h.DestroyApplication)
	cs.PATCH(":id", h.UpdateApplication)
	cs.GET(":id", h.GetApplication)
	cs.GET("", h.GetApplications)

	// Model.
	model := apiv1.Group("/models", jwt.MiddlewareFunc(), rbac)
	model.DELETE(":id", h.DestroyModel)
	model.PATCH(":id", h.UpdateModel)
	model.GET(":id", h.GetModel)
	model.GET("", h.GetModels)

	// Personal Access Token.
	pat := apiv1.Group("/personal-access-tokens", jwt.MiddlewareFunc(), rbac)
	pat.POST("", h.CreatePersonalAccessToken)
	pat.DELETE(":id", h.DestroyPersonalAccessToken)
	pat.PATCH(":id", h.UpdatePersonalAccessToken)
	pat.GET(":id", h.GetPersonalAccessToken)
	pat.GET("", h.GetPersonalAccessTokens)

	// Open API router.
	oapiv1 := r.Group("/oapi/v1")

	// Job.
	ojob := oapiv1.Group("/jobs", personalAccessToken)
	ojob.POST("", h.CreateJob)
	ojob.DELETE(":id", h.DestroyJob)
	ojob.PATCH(":id", h.UpdateJob)
	ojob.GET(":id", h.GetJob)
	ojob.GET("", h.GetJobs)

	// Cluster.
	oc := oapiv1.Group("/clusters", personalAccessToken)
	oc.POST("", h.CreateCluster)
	oc.DELETE(":id", h.DestroyCluster)
	oc.PATCH(":id", h.UpdateCluster)
	oc.GET(":id", h.GetCluster)
	oc.GET("", h.GetClusters)

	// TODO Remove this api.
	// Compatible with the V1 preheat.
	pv1 := r.Group("/preheats")
	r.GET("_ping", h.GetHealth)
	pv1.POST("", h.CreateV1Preheat)
	pv1.GET(":id", h.GetV1Preheat)

	// Health Check.
	r.GET("/healthy", h.GetHealth)

	// Swagger.
	apiSeagger := ginSwagger.URL("/swagger/doc.json")
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, apiSeagger))

	// Fallback to manager view.
	r.NoRoute(func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/")
	})

	return r, nil
}
