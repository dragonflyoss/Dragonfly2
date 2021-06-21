package server

import (
	// manager swag api
	_ "d7y.io/dragonfly/v2/api/v2/manager"
	"d7y.io/dragonfly/v2/manager/handlers"
	"d7y.io/dragonfly/v2/manager/middlewares"
	"d7y.io/dragonfly/v2/manager/service"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func initRouter(verbose bool, service service.Service) (*gin.Engine, error) {
	// Set mode
	if verbose == false {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	h := handlers.NewHandler(service)
	r.Use(middlewares.Error())

	apiv1 := r.Group("/api/v1")
	sc := apiv1.Group("/schedulers")
	sc.POST("", h.CreateScheduler)
	sc.DELETE(":id", h.DestroyScheduler)
	sc.PATCH(":id", h.UpdateScheduler)
	sc.GET(":id", h.GetScheduler)
	sc.GET("", h.GetSchedulers)

	// si := apiv1.Group("/schedulers/:schedulerId/instances")
	// si.POST("", h.CreateSchedulerInstance)
	// si.DELETE(":id", h.DestroySchedulerInstance)
	// si.PATCH(":id", h.UpdateSchedulerInstance)
	// si.GET(":id", h.GetSchedulerInstance)
	// si.GET("", h.GetSchedulerInstances)

	cc := apiv1.Group("/cdns")
	cc.POST("", h.CreateCDN)
	cc.DELETE(":id", h.DestroyCDN)
	cc.PATCH(":id", h.UpdateCDN)
	cc.GET(":id", h.GetCDN)
	cc.GET("", h.GetCDNs)

	// ci := apiv1.Group("/cdns/:cdnId/instances")
	// ci.POST("", h.CreateCDNInstance)
	// ci.DELETE(":id", h.DestroyCDNInstance)
	// ci.PATCH(":id", h.UpdateCDNInstance)
	// ci.GET(":id", h.GetCDNInstance)
	// ci.GET("", h.GetCDNInstances)

	sg := apiv1.Group("/security-groups")
	sg.POST("", h.CreateSecurityGroup)
	sg.DELETE(":id", h.DestroySecurityGroup)
	sg.PATCH(":id", h.UpdateSecurityGroup)
	sg.GET(":id", h.GetSecurityGroup)
	sg.GET("", h.GetSecurityGroups)

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return r, nil
}
