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

	api := router.Group("/api/v2")
	{
		{
			configs := api.Group("/schedulerclusters")
			configs.POST("", handler.AddSchedulerCluster)
			configs.DELETE(":id", handler.DeleteSchedulerCluster)
			configs.POST(":id", handler.UpdateSchedulerCluster)
			configs.GET(":id", handler.GetSchedulerCluster)
			configs.GET("", handler.ListSchedulerClusters)
		}

		{
			configs := api.Group("/schedulerinstances")
			configs.POST("", handler.AddSchedulerInstance)
			configs.DELETE(":id", handler.DeleteSchedulerInstance)
			configs.POST(":id", handler.UpdateSchedulerInstance)
			configs.GET(":id", handler.GetSchedulerInstance)
			configs.GET("", handler.ListSchedulerInstances)
		}

		{
			configs := api.Group("/cdnclusters")
			configs.POST("", handler.AddCDNCluster)
			configs.DELETE(":id", handler.DeleteCDNCluster)
			configs.POST(":id", handler.UpdateCDNCluster)
			configs.GET(":id", handler.GetCDNCluster)
			configs.GET("", handler.ListCDNClusters)
		}

		{
			configs := api.Group("/cdninstances")
			configs.POST("", handler.AddCDNInstance)
			configs.DELETE(":id", handler.DeleteCDNInstance)
			configs.POST(":id", handler.UpdateCDNInstance)
			configs.GET(":id", handler.GetCDNInstance)
			configs.GET("", handler.ListCDNInstances)
		}
	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return router, nil
}
