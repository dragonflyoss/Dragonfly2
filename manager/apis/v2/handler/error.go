package handler

import (
	"fmt"
	"net/http"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
)

func (handler *Handler) Error(ctx *gin.Context) {
	ctx.Next()
	e := ctx.Errors.Last()
	if e != nil {
		err, ok := e.Err.(*HTTPError)
		if !ok {
			httpErr := &HTTPError{
				Code:    http.StatusNotFound,
				Message: err.Error(),
			}
			ctx.JSON(httpErr.Code, httpErr)
			return
		}
		ctx.JSON(err.Code, err)
	}
}

// NewError example
func NewError(status int, err error) error {
	httpErr := &HTTPError{
		Message: err.Error(),
	}

	if status > 0 {
		httpErr.Code = status
		return httpErr
	}
	e, ok := err.(*dferrors.DfError)
	if ok {
		switch e.Code {
		case dfcodes.InvalidResourceType:
			httpErr.Code = http.StatusBadRequest
		case dfcodes.ManagerStoreError:
			httpErr.Code = http.StatusInternalServerError
		case dfcodes.ManagerStoreNotFound:
			httpErr.Code = http.StatusNotFound
		default:
			httpErr.Code = http.StatusNotFound
		}
	} else {
		httpErr.Code = http.StatusNotFound

	}
	return httpErr
}

// HTTPError example
type HTTPError struct {
	Code    int    `json:"code" example:"400"`
	Message string `json:"message" example:"status bad request"`
}

func (h *HTTPError) Error() string {
	return fmt.Sprintf("code:%d messsage:%s", h.Code, h.Message)
}
