package handler

import (
	"fmt"
	"net/http"

	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
)

func (handler *Handler) Error(ctx *gin.Context) {
	ctx.Next()
	e := ctx.Errors.Last()
	if e != nil {
		err, ok := e.Err.(*HTTPError)
		if !ok {
			ctx.JSON(http.StatusNotFound, err)
			return
		}
		ctx.JSON(err.Code, err)
	}
}

// NewError example
func NewError(ctx *gin.Context, status int, err error) {
	er := HTTPError{
		Message: err.Error(),
	}

	if status > 0 {
		er.Code = status
		ctx.JSON(er.Code, er)
		return
	}
	e, ok := err.(*dferrors.DfError)
	if ok {
		er.Code = int(e.Code)
	} else {
		er.Code = http.StatusNotFound

	}
	ctx.JSON(er.Code, er)
	return
}

func NewHTTPError(status int, err error) error {
	httpErr := &HTTPError{
		Message: err.Error(),
	}

	if status > 0 {
		httpErr.Code = status
		return httpErr
	}
	e, ok := err.(*dferrors.DfError)
	if ok {
		httpErr.Code = int(e.Code)
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
