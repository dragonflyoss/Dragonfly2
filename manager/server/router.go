package server

import (
	"d7y.io/dragonfly/v2/manager/handlers"
	"d7y.io/dragonfly/v2/manager/middlewares"
	"d7y.io/dragonfly/v2/manager/service"
	"github.com/gin-gonic/gin"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
)

func initRouter(verbose bool, service service.REST) (*gin.Engine, error) {
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

	jwt, err := middlewares.Jwt(service)
	if err != nil {
		return nil, err
	}

	// Router
	apiv1 := r.Group("/api/v1")

	// User
	u := apiv1.Group("/users")
	u.POST("/signin", jwt.LoginHandler)
	u.POST("/signout", jwt.LogoutHandler)
	u.POST("/refresh_token", jwt.RefreshHandler)
	u.POST("/signup", h.SignUp)

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
	return r, nil
}
