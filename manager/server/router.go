package server

import (
	_ "d7y.io/dragonfly/v2/api/v2/manager"
	"d7y.io/dragonfly/v2/manager/apis/v2manager/handler"
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
		configs := api.Group("/configs")
		{
			configs.POST("", handler.AddConfig)
			configs.DELETE(":id", handler.DeleteConfig)
			configs.POST(":id", handler.UpdateConfig)
			configs.GET(":id", handler.GetConfig)
			configs.GET("", handler.ListConfigs)
		}
	}

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return router, nil
}
