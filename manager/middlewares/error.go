package middlewares

import (
	"net/http"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type ErrorResponse struct {
	Message     string `json:"message,omitempty"`
	Error       string `json:"errors,omitempty"`
	DocumentURL string `json:"documentation_url,omitempty"`
}

func Error() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		err := c.Errors.Last()
		if err == nil {
			return
		}

		// Gin error handler
		if err, ok := errors.Cause(err.Err).(*gin.Error); ok {
			switch err.Type {
			case gin.ErrorTypeBind:
				c.JSON(http.StatusUnprocessableEntity, ErrorResponse{
					Message: http.StatusText(http.StatusUnprocessableEntity),
					Error:   err.Error(),
				})
				return
			default:
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Message: http.StatusText(http.StatusInternalServerError),
				})
				return
			}
		}

		// RPC error handler
		if err, ok := errors.Cause(err.Err).(*dferrors.DfError); ok {
			switch err.Code {
			case dfcodes.InvalidResourceType:
				c.JSON(http.StatusBadRequest, ErrorResponse{
					Message: http.StatusText(http.StatusBadRequest),
				})
				return
			default:
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Message: http.StatusText(http.StatusInternalServerError),
				})
				return
			}
		}

		// GORM ErrRecordNotFound handler
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Message: http.StatusText(http.StatusNotFound),
			})
			return
		}

		// Unknown error
		c.JSON(http.StatusInternalServerError, nil)
	}
}
