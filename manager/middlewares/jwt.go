package middlewares

import (
	"net/http"
	"time"

	"d7y.io/dragonfly/v2/manager/handlers"
	"d7y.io/dragonfly/v2/manager/types"
	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
)

func Jwt(h *handlers.Handlers) (*jwt.GinJWTMiddleware, error) {
	authMiddleware, err := jwt.New(&jwt.GinJWTMiddleware{
		Realm:      "Dragonfly Zone",
		Key:        []byte("Dragonfly Secret Key"),
		Timeout:    time.Hour,
		MaxRefresh: time.Hour,
		Authenticator: func(c *gin.Context) (interface{}, error) {
			var loginInfos types.LoginRequest
			if err := c.ShouldBind(&loginInfos); err != nil {
				return "", jwt.ErrMissingLoginValues
			}
			userInfo, err := h.Service.Login(loginInfos)
			if err != nil {
				return "", jwt.ErrFailedAuthentication
			}

			return map[string]interface{}{
				"name": userInfo.Name,
			}, nil
		},
		LoginResponse: func(c *gin.Context, code int, token string, expire time.Time) {
			c.JSON(http.StatusOK, gin.H{
				"token":  token,
				"expire": expire.Format(time.RFC3339),
			})
		},
		PayloadFunc: func(data interface{}) jwt.MapClaims {
			if v, ok := data.(map[string]interface{}); ok {
				return jwt.MapClaims{
					"name": v["name"],
				}
			}
			return jwt.MapClaims{}
		},

		Unauthorized: func(c *gin.Context, code int, message string) {
			c.JSON(code, gin.H{
				"message": message,
			})
		},
		TokenLookup:    "header: Authorization, query: token, cookie: jwt",
		TokenHeadName:  "Bearer",
		TimeFunc:       time.Now,
		SendCookie:     true,
		CookieHTTPOnly: true,
	})

	if err != nil {
		return nil, err
	}

	return authMiddleware, nil

}
