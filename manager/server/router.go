package server

import (
	"d7y.io/dragonfly/v2/manager/handlers"
	"d7y.io/dragonfly/v2/manager/middlewares"
	"d7y.io/dragonfly/v2/manager/service"
	"github.com/gin-gonic/gin"
	ginprometheus "github.com/mcuadros/go-gin-prometheus"
)

func initRouter(verbose bool, service service.Service) (*gin.Engine, error) {
	// Set mode
	if verbose == false {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	h := handlers.NewHandler(service)

	// Prometheus
	p := ginprometheus.NewPrometheus("kore_apiserver")
	p.Use(r)

	// Middleware
	r.Use(gin.Logger())
	r.Use(gin.Recovery())
	r.Use(middlewares.Error())

	// Router
	apiv1 := r.Group("/api/v1")

	// Scheduler
	sc := apiv1.Group("/schedulers")
	sc.POST("", h.CreateScheduler)
	sc.DELETE(":id", h.DestroyScheduler)
	sc.PATCH(":id", h.UpdateScheduler)
	sc.GET(":id", h.GetScheduler)
	sc.GET("", h.GetSchedulers)
	sc.PUT(":id/scheduler-instances/:instance_id", h.AddInstanceToScheduler)

	// Scheduler Instance
	si := apiv1.Group("/scheduler-instances")
	si.POST("", h.CreateSchedulerInstance)
	si.DELETE(":id", h.DestroySchedulerInstance)
	si.PATCH(":id", h.UpdateSchedulerInstance)
	si.GET(":id", h.GetSchedulerInstance)
	si.GET("", h.GetSchedulerInstances)

	// CDN
	cc := apiv1.Group("/cdns")
	cc.POST("", h.CreateCDN)
	cc.DELETE(":id", h.DestroyCDN)
	cc.PATCH(":id", h.UpdateCDN)
	cc.GET(":id", h.GetCDN)
	cc.GET("", h.GetCDNs)
	cc.PUT(":id/cdn-instances/:instance_id", h.AddInstanceToCDN)
	cc.PUT(":id/schedulers/:scheduler_id", h.AddSchedulerToCDN)

	// CDN Instance
	ci := apiv1.Group("/cdn-instances")
	ci.POST("", h.CreateCDNInstance)
	ci.DELETE(":id", h.DestroyCDNInstance)
	ci.PATCH(":id", h.UpdateCDNInstance)
	ci.GET(":id", h.GetCDNInstance)
	ci.GET("", h.GetCDNInstances)

	// Security Group
	sg := apiv1.Group("/security-groups")
	sg.POST("", h.CreateSecurityGroup)
	sg.DELETE(":id", h.DestroySecurityGroup)
	sg.PATCH(":id", h.UpdateSecurityGroup)
	sg.GET(":id", h.GetSecurityGroup)
	sg.GET("", h.GetSecurityGroups)
	sg.PUT(":id/scheduler-instances/:instance_id", h.AddSchedulerInstanceToSecurityGroup)
	sg.PUT(":id/cdn-instances/:instance_id", h.AddCDNInstanceToSecurityGroup)

	// Health Check
	r.GET("/healthy/*action", h.GetHealth)
	return r, nil
}
