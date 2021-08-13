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
	"io"
	"os"
	"path/filepath"

	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/manager/handlers"
	"d7y.io/dragonfly/v2/manager/middlewares"
	"d7y.io/dragonfly/v2/manager/service"
	"github.com/casbin/casbin/v2"
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

const (
	GinLogFileName = "gin.log"
)

func Init(console bool, verbose bool, publicPath string, service service.REST, enforcer *casbin.Enforcer) (*gin.Engine, error) {
	// Set mode
	if !verbose {
		gin.SetMode(gin.ReleaseMode)
	}

	// Logging to a file
	if !console {
		gin.DisableConsoleColor()
		logDir := filepath.Join(dfpath.LogDir, "manager")
		f, _ := os.Create(filepath.Join(logDir, GinLogFileName))
		gin.DefaultWriter = io.MultiWriter(f)
	}

	r := gin.New()
	h := handlers.New(service)

	// Prometheus
	p := ginprometheus.NewPrometheus("dragonfly_manager")
	p.Use(r)

	// CORS
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true

	// Middleware
	r.Use(gin.Logger())
	r.Use(gin.Recovery())
	r.Use(middlewares.Error())
	r.Use(cors.New(corsConfig))

	rbac := middlewares.RBAC(enforcer)
	jwt, err := middlewares.Jwt(service)
	if err != nil {
		return nil, err
	}

	// Router
	apiv1 := r.Group("/api/v1")

	// User
	ai := apiv1.Group("/users")
	ai.POST("/signin", jwt.LoginHandler)
	ai.POST("/signout", jwt.LogoutHandler)
	ai.POST("/refresh_token", jwt.RefreshHandler)
	ai.POST("/:id/roles/:role_name", h.AddRoleToUser)
	ai.DELETE("/:id/roles/:role_name", h.DeleteRoleForUser)
	ai.POST("/signup", h.SignUp)

	// Scheduler Cluster
	sc := apiv1.Group("/scheduler-clusters")
	sc.POST("", h.CreateSchedulerCluster)
	sc.DELETE(":id", h.DestroySchedulerCluster)
	sc.PATCH(":id", h.UpdateSchedulerCluster)
	sc.GET(":id", h.GetSchedulerCluster)
	sc.GET("", h.GetSchedulerClusters)
	sc.PUT(":id/schedulers/:scheduler_id", h.AddSchedulerToSchedulerCluster)

	// Scheduler
	s := apiv1.Group("/schedulers")
	s.POST("", h.CreateScheduler)
	s.DELETE(":id", h.DestroyScheduler)
	s.PATCH(":id", h.UpdateScheduler)
	s.GET(":id", h.GetScheduler)
	s.GET("", h.GetSchedulers)

	// Settings
	st := apiv1.Group("/settings")
	st.POST("", h.CreateSetting)
	st.DELETE(":id", h.DestroySetting)
	st.PATCH("", h.UpdateSetting)
	st.GET("", h.GetSettings)

	// CDN Cluster
	cc := apiv1.Group("/cdn-clusters")
	cc.POST("", h.CreateCDNCluster)
	cc.DELETE(":id", h.DestroyCDNCluster)
	cc.PATCH(":id", h.UpdateCDNCluster)
	cc.GET(":id", h.GetCDNCluster)
	cc.GET("", h.GetCDNClusters)
	cc.PUT(":id/cdns/:cdn_id", h.AddCDNToCDNCluster)
	cc.PUT(":id/scheduler-clusters/:scheduler_cluster_id", h.AddSchedulerClusterToCDNCluster)

	// CDN
	c := apiv1.Group("/cdns")
	c.POST("", h.CreateCDN)
	c.DELETE(":id", h.DestroyCDN)
	c.PATCH(":id", h.UpdateCDN)
	c.GET(":id", h.GetCDN)
	c.GET("", h.GetCDNs)

	// Role
	re := apiv1.Group("/roles", jwt.MiddlewareFunc(), rbac)
	re.POST("", h.CreateRole)
	re.GET("", h.GetRoles)
	re.DELETE("/:role_name", h.DestroyRole)
	re.GET("/:role_name", h.GetRole)
	re.POST("/:role_name/permission", h.AddRolePermission)
	re.DELETE("/:role_name/permission", h.RemoveRolePermission)

	// Permission
	pn := apiv1.Group("/permissions", jwt.MiddlewareFunc(), rbac)
	pn.GET("", h.GetPermissions(r))

	// oauth
	oa := apiv1.Group("/oauth")
	oa.GET("", h.GetOauths)
	oa.GET("/:id", h.GetOauth)
	oa.DELETE("/:id", h.DestroyOauth)
	oa.PUT("/:id", h.UpdateOauth)
	oa.POST("", h.CreateOauth)
	oa.GET("/signin/:oauth_name", h.OauthSignin)
	oa.GET("/callback/:oauth_name", h.OauthCallback(jwt))

	// Security Group
	sg := apiv1.Group("/security-groups")
	sg.POST("", h.CreateSecurityGroup)
	sg.DELETE(":id", h.DestroySecurityGroup)
	sg.PATCH(":id", h.UpdateSecurityGroup)
	sg.GET(":id", h.GetSecurityGroup)
	sg.GET("", h.GetSecurityGroups)
	sg.PUT(":id/scheduler-clusters/:scheduler_cluster_id", h.AddSchedulerClusterToSecurityGroup)
	sg.PUT(":id/cdn-clusters/:cdn_cluster_id", h.AddCDNClusterToSecurityGroup)

	// Preheat
	ph := apiv1.Group("/preheats")
	ph.POST("", h.CreatePreheat)
	ph.GET(":id", h.GetPreheat)

	// Health Check
	r.GET("/healthy/*action", h.GetHealth)

	// Manager View
	r.Use(static.Serve("/", static.LocalFile(publicPath, false)))

	// Swagger
	apiSeagger := ginSwagger.URL("/swagger/doc.json")
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, apiSeagger))

	return r, nil
}
