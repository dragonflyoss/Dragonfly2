package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// @Summary Create Oauth
// @Description create by json config
// @Tags Oauth
// @Accept json
// @Produce json
// @Param Oauth body types.CreateOauthRequest true "Oauth"
// @Success 200 {object} model.Oauth
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /oauths [post]
func (h *Handlers) CreateOauth(ctx *gin.Context) {
	var json types.CreateOauthRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauth, err := h.Service.CreateOauth(json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, oauth)
}

// @Summary Destroy Oauth
// @Description Destroy by id
// @Tags Oauth
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /oauths/{id} [delete]
func (h *Handlers) DestroyOauth(ctx *gin.Context) {
	var params types.OauthParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	err := h.Service.DestroyOauth(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Oauth
// @Description Update by json config
// @Tags Oauth
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Oauth body types.UpdateOauthRequest true "Oauth"
// @Success 200 {object} model.Oauth
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /oauth/{id} [patch]
func (h *Handlers) UpdateOauth(ctx *gin.Context) {
	var params types.OauthParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateOauthRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	oauth, err := h.Service.UpdateOauth(params.ID, json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, oauth)
}

// @Summary Get Oauth
// @Description Get Oauth by id
// @Tags Oauth
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.Oauth
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /oauth/{id} [get]
func (h *Handlers) GetOauth(ctx *gin.Context) {
	var params types.OauthParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauth, err := h.Service.GetOauth(params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, oauth)
}

// @Summary Get Oauths
// @Description Get Oauths
// @Tags Oauth
// @Accept json
// @Produce json
// @Success 200 {object} []model.Oauth
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /oauth [get]
func (h *Handlers) GetOauths(ctx *gin.Context) {
	oauths, err := h.Service.GetOauths()
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, oauths)
}
