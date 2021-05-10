package main

import (
	"d7y.io/dragonfly/v2/manager/apis/v2/handler"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/server/service"
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
	server := service.NewManagerServer(config.New())
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
	router.Run(":8080")
}
