package middlewares

import (
	"net/http"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/VividCortex/mysqlerr"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
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

		// GORM error handler
		if errors.Is(err.Err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{
				Message: http.StatusText(http.StatusNotFound),
			})
			return
		}

		// Mysql error handler
		if err, ok := errors.Cause(err.Err).(*mysql.MySQLError); ok {
			switch err.Number {
			case mysqlerr.ER_DUP_ENTRY:
				c.JSON(http.StatusConflict, ErrorResponse{
					Message: http.StatusText(http.StatusConflict),
				})
				return
			default:
				c.JSON(http.StatusInternalServerError, ErrorResponse{
					Message: http.StatusText(http.StatusInternalServerError),
				})
				return
			}
		}

		// Unknown error
		c.JSON(http.StatusInternalServerError, nil)
	}
}
