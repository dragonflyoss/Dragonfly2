package handler

import (
	"net/http"

	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
)

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

// HTTPError example
type HTTPError struct {
	Code    int    `json:"code" example:"400"`
	Message string `json:"message" example:"status bad request"`
}
