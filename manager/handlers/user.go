package handlers

import (
	"net/http"

	"d7y.io/dragonfly.v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Register user
// @Description Register user by json config
// @Tags User
// @Accept json
// @Produce json
// @Param User body types.RegisterRequest true "User"
// @Success 200 {object} model.User
// @Failure 400 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /auth/register [post]
func (h *Handlers) Register(ctx *gin.Context) {
	var json types.RegisterRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	userInfo, err := h.Service.Register(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, userInfo)
}
