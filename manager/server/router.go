package server

import (
	// manager swag api
	_ "d7y.io/dragonfly/v2/api/v2/manager"
	"d7y.io/dragonfly/v2/manager/apis/v2/handler"
	"d7y.io/dragonfly/v2/manager/server/service"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func InitRouter(server *service.ManagerServer) (*gin.Engine, error) {
	router := gin.New()
	handler := handler.NewHandler(server)

	router.Use(handler.Error)

	api := router.Group("/api/v2")
	{
		{
			configs := api.Group("/scheduler/clusters")
			configs.POST("", handler.CreateSchedulerCluster)
			configs.DELETE(":id", handler.DestroySchedulerCluster)
			configs.PATCH(":id", handler.UpdateSchedulerCluster)
			configs.GET(":id", handler.GetSchedulerCluster)
			configs.GET("", handler.ListSchedulerClusters)
		}

		{
			configs := api.Group("/scheduler/instances")
			configs.POST("", handler.CreateSchedulerInstance)
			configs.DELETE(":id", handler.DestroySchedulerInstance)
			configs.PATCH(":id", handler.UpdateSchedulerInstance)
			configs.GET(":id", handler.GetSchedulerInstance)
			configs.GET("", handler.ListSchedulerInstances)
		}

		{
			configs := api.Group("/cdn/clusters")
			configs.POST("", handler.CreateCDNCluster)
			configs.DELETE(":id", handler.DestroyCDNCluster)
			configs.PATCH(":id", handler.UpdateCDNCluster)
			configs.GET(":id", handler.GetCDNCluster)
			configs.GET("", handler.ListCDNClusters)
		}

		{
			configs := api.Group("/cdn/instances")
			configs.POST("", handler.CreateCDNInstance)
			configs.DELETE(":id", handler.DestroyCDNInstance)
			configs.PATCH(":id", handler.UpdateCDNInstance)
			configs.GET(":id", handler.GetCDNInstance)
			configs.GET("", handler.ListCDNInstances)
		}
	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return router, nil
}
