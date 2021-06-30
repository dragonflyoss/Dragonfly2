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
	if verbose == false {
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

	// Router
	apiv1 := r.Group("/api/v1")

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
	return r, nil
}
