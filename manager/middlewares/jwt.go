/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package middlewares

import (
	"net/http"
	"time"

	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/service"
	"d7y.io/dragonfly/v2/manager/types"
	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
)

type user struct {
	userName string
	ID       uint
}

func Jwt(service service.REST) (*jwt.GinJWTMiddleware, error) {
	var identityKey = "username"
	authMiddleware, err := jwt.New(&jwt.GinJWTMiddleware{
		Realm:       "Dragonfly",
		Key:         []byte("Secret Key"),
		Timeout:     time.Hour,
		MaxRefresh:  time.Hour,
		IdentityKey: identityKey,

		IdentityHandler: func(c *gin.Context) interface{} {
			claims := jwt.ExtractClaims(c)
			userNmae, ok := claims[identityKey]
			if !ok {
				c.JSON(http.StatusUnauthorized, gin.H{
					"message": "Unavailable token: require username info",
				})
				c.Abort()
				return nil
			}
			userID, ok := claims["ID"]
			if !ok {
				c.JSON(http.StatusUnauthorized, gin.H{
					"message": "Unavailable token: require id info",
				})
				c.Abort()
				return nil
			}
			u := &user{
				userName: userNmae.(string),
				ID:       userID.(uint),
			}
			c.Set("userName", u.userName)
			c.Set("userID", u.ID)
			return u
		},

		Authenticator: func(c *gin.Context) (interface{}, error) {
			var json types.SignInRequest
			if err := c.ShouldBind(&json); err != nil {
				return "", jwt.ErrMissingLoginValues
			}

			u, err := service.SignIn(json)
			if err != nil {
				return "", jwt.ErrFailedAuthentication
			}

			return u, nil
		},

		PayloadFunc: func(data interface{}) jwt.MapClaims {
			if u, ok := data.(*model.User); ok {
				return jwt.MapClaims{
					identityKey: u.Name,
					"ID":        u.ID,
				}
			}
			return jwt.MapClaims{}
		},

		Unauthorized: func(c *gin.Context, code int, message string) {
			c.JSON(code, gin.H{
				"message": message,
			})
		},

		LoginResponse: func(c *gin.Context, code int, token string, expire time.Time) {
			c.JSON(code, gin.H{
				"token":  token,
				"expire": expire.Format(time.RFC3339),
			})
		},

		LogoutResponse: func(c *gin.Context, code int) {
			c.Status(code)
		},

		RefreshResponse: func(c *gin.Context, code int, token string, expire time.Time) {
			c.JSON(code, gin.H{
				"token":  token,
				"expire": expire.Format(time.RFC3339),
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
