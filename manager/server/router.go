package server

import (
	"d7y.io/dragonfly/v2/manager/handlers"
	"d7y.io/dragonfly/v2/manager/middlewares"
	rbacbase "d7y.io/dragonfly/v2/manager/permission/rbac"
	"d7y.io/dragonfly/v2/manager/service"
	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
)

func initRouter(verbose bool, service service.REST, enforcer *casbin.Enforcer) (*gin.Engine, error) {
	// Set mode
	if !verbose {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	h := handlers.New(service)

	// Prometheus
	p := ginprometheus.NewPrometheus("dragonfly_manager")
	p.Use(r)

	// Middleware
	r.Use(gin.Logger())
	r.Use(gin.Recovery())
	r.Use(middlewares.Error())

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
	ai.POST("/signup", jwt.MiddlewareFunc(), rbac, h.SignUp)

	// Scheduler Cluster
	sc := apiv1.Group("/scheduler-clusters")
	sc.POST("", h.CreateSchedulerCluster)
	sc.DELETE(":id", h.DestroySchedulerCluster)
	sc.PATCH(":id", h.UpdateSchedulerCluster)
	sc.GET(":id", h.GetSchedulerCluster)
	sc.GET("", h.GetSchedulerClusters)
	sc.PUT(":id/schedulers/:scheduler_id", h.AddSchedulerToSchedulerCluster)

	// Scheduler
	si := apiv1.Group("/schedulers")
	si.POST("", h.CreateScheduler)
	si.DELETE(":id", h.DestroyScheduler)
	si.PATCH(":id", h.UpdateScheduler)
	si.GET(":id", h.GetScheduler)
	si.GET("", h.GetSchedulers)

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
	ci := apiv1.Group("/cdns")
	ci.POST("", h.CreateCDN)
	ci.DELETE(":id", h.DestroyCDN)
	ci.PATCH(":id", h.UpdateCDN)
	ci.GET(":id", h.GetCDN)
	ci.GET("", h.GetCDNs)

	// Permission
	pn := apiv1.Group("/permission", jwt.MiddlewareFunc(), rbac)
	pn.POST("", h.CreatePermission)
	pn.DELETE("", h.DestroyPermission)
	pn.GET("/groups", h.GetPermissionGroups(r))
	pn.GET("/:subject", h.GetRolesForUser)
	pn.GET("/:subject/:object/:action", h.HasRoleForUser)

	// Security Group
	sg := apiv1.Group("/security-groups")
	sg.POST("", h.CreateSecurityGroup)
	sg.DELETE(":id", h.DestroySecurityGroup)
	sg.PATCH(":id", h.UpdateSecurityGroup)
	sg.GET(":id", h.GetSecurityGroup)
	sg.GET("", h.GetSecurityGroups)
	sg.PUT(":id/scheduler-clusters/:scheduler_cluster_id", h.AddSchedulerClusterToSecurityGroup)
	sg.PUT(":id/cdn-clusters/:cdn_cluster_id", h.AddCDNClusterToSecurityGroup)

	// Health Check
	r.GET("/healthy/*action", h.GetHealth)

	// auto init role checn roles
	err = rbacbase.InitRole(enforcer, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}
