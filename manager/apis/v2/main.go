package main

import (
	"os"

	"d7y.io/dragonfly/v2/manager/apis/v2/handler"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/server/service"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	// manager swag api
	_ "d7y.io/dragonfly/v2/api/v2/manager"
)

// @title Swagger Example API
// @version 1.0
// @description This is a sample server Petstore server.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8080
// @BasePath /api/v2
func main() {
	server, err := service.NewManagerServer(config.New())
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}

	router := gin.New()
	handler := handler.NewHandler(server)

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
	router.Run(":8080")
}
