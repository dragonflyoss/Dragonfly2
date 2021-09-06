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

	// Manager View
	r.Use(static.Serve("/", static.LocalFile(publicPath, true)))

	// Router
	apiv1 := r.Group("/api/v1")

	// User
	u := apiv1.Group("/users")
	u.GET("/:id", jwt.MiddlewareFunc(), rbac, h.GetUser)
	u.POST("/signin", jwt.LoginHandler)
	u.POST("/signout", jwt.LogoutHandler)
	u.POST("/signup", h.SignUp)
	u.GET("/signin/:name", h.OauthSignin)
	u.GET("/signin/:name/callback", h.OauthSigninCallback(jwt))
	u.POST("/refresh_token", jwt.RefreshHandler)
	u.POST("/:id/reset_password", h.ResetPassword)
	u.GET("/:id/roles", jwt.MiddlewareFunc(), rbac, h.GetRolesForUser)
	u.PUT("/:id/roles/:role", jwt.MiddlewareFunc(), rbac, h.AddRoleToUser)
	u.DELETE("/:id/roles/:role", jwt.MiddlewareFunc(), rbac, h.DeleteRoleForUser)

	// Role
	re := apiv1.Group("/roles", jwt.MiddlewareFunc(), rbac)
	re.POST("", h.CreateRole)
	re.DELETE("/:role", h.DestroyRole)
	re.GET("/:role", h.GetRole)
	re.GET("", h.GetRoles)
	re.POST("/:role/permissions", h.AddPermissionForRole)
	re.DELETE("/:role/permissions", h.DeletePermissionForRole)

	// Permission
	pm := apiv1.Group("/permissions", jwt.MiddlewareFunc(), rbac)
	pm.GET("", h.GetPermissions(r))

	// Oauth
	oa := apiv1.Group("/oauth", jwt.MiddlewareFunc(), rbac)
	oa.POST("", h.CreateOauth)
	oa.DELETE(":id", h.DestroyOauth)
	oa.PATCH(":id", h.UpdateOauth)
	oa.GET(":id", h.GetOauth)
	oa.GET("", h.GetOauths)

	// Scheduler Cluster
	sc := apiv1.Group("/scheduler-clusters", jwt.MiddlewareFunc(), rbac)
	sc.POST("", h.CreateSchedulerCluster)
	sc.DELETE(":id", h.DestroySchedulerCluster)
	sc.PATCH(":id", h.UpdateSchedulerCluster)
	sc.GET(":id", h.GetSchedulerCluster)
	sc.GET("", h.GetSchedulerClusters)
	sc.PUT(":id/schedulers/:scheduler_id", h.AddSchedulerToSchedulerCluster)

	// Scheduler
	s := apiv1.Group("/schedulers", jwt.MiddlewareFunc(), rbac)
	s.POST("", h.CreateScheduler)
	s.DELETE(":id", h.DestroyScheduler)
	s.PATCH(":id", h.UpdateScheduler)
	s.GET(":id", h.GetScheduler)
	s.GET("", h.GetSchedulers)

	// CDN Cluster
	cc := apiv1.Group("/cdn-clusters", jwt.MiddlewareFunc(), rbac)
	cc.POST("", h.CreateCDNCluster)
	cc.DELETE(":id", h.DestroyCDNCluster)
	cc.PATCH(":id", h.UpdateCDNCluster)
	cc.GET(":id", h.GetCDNCluster)
	cc.GET("", h.GetCDNClusters)
	cc.PUT(":id/cdns/:cdn_id", h.AddCDNToCDNCluster)
	cc.PUT(":id/scheduler-clusters/:scheduler_cluster_id", h.AddSchedulerClusterToCDNCluster)

	// CDN
	c := apiv1.Group("/cdns", jwt.MiddlewareFunc(), rbac)
	c.POST("", h.CreateCDN)
	c.DELETE(":id", h.DestroyCDN)
	c.PATCH(":id", h.UpdateCDN)
	c.GET(":id", h.GetCDN)
	c.GET("", h.GetCDNs)

	// Security Group
	sg := apiv1.Group("/security-groups", jwt.MiddlewareFunc(), rbac)
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

	// Swagger
	apiSeagger := ginSwagger.URL("/swagger/doc.json")
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, apiSeagger))

	// Fallback To Manager View
	r.NoRoute(func(c *gin.Context) {
		c.File(filepath.Join(publicPath, "index.html"))
	})

	return r, nil
}
