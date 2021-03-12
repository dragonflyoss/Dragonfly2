package main

import (
	"d7y.io/dragonfly/v2/manager/apis/v2manager/handler"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/server/service"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "d7y.io/dragonfly/v2/api/v2/manager/docs"
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

// @host petstore.swagger.io:8080
// @BasePath /api/v2
func main() {
	server := service.NewManagerServer(config.GetConfig())
	router := gin.New()
	handler := handler.NewHandler(server)

	api := router.Group("/api/v2")
	{
		configs := api.Group("/configs")
		{
			configs.POST("", handler.AddConfig)
			configs.DELETE(":id", handler.DeleteConfig)
			configs.POST(":id", handler.UpdateConfig)
			configs.GET("/:id", handler.GetConfig)
			configs.GET("", handler.ListConfigs)
		}
	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	router.Run(":8080")
}
