package server

import (
	_ "d7y.io/dragonfly/v2/api/v2/manager/docs"
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
		configs := api.Group("/schedulerclusters")
		{
			configs.POST("", handler.AddSchedulerCluster)
			configs.DELETE(":id", handler.DeleteSchedulerCluster)
			configs.POST(":id", handler.UpdateSchedulerCluster)
			configs.GET(":id", handler.GetSchedulerCluster)
			configs.GET("", handler.ListSchedulerClusters)
		}
	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return router, nil
}
