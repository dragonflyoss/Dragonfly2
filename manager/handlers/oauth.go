package handlers

import (
	"net/http"

	"d7y.io/dragonfly/v2/manager/types"
	jwt "github.com/appleboy/gin-jwt/v2"
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

// @Summary start oauth signin
// @Description start oauth signin
// @Tags Oauth
// @Accept json
// @Produce json
// @Param oauth_name path string true "oauth_name"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /{oauth_name}/sigin [get]
func (h *Handlers) OauthSignin(ctx *gin.Context) {
	var params types.OauthPathParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	oauthURL, err := h.Service.OauthSignin(params.OauthName)
	if err != nil {
		ctx.Error(err)
		return
	}
	ctx.Redirect(http.StatusMovedPermanently, oauthURL)
}

// @Summary start oauth callback
// @Description start oauth callback
// @Tags Oauth
// @Param oauth_name path string true "oauth_name"
// @Param code query string true "code"
// @Success 200
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /{oauth_name}/callback [get]
func (h *Handlers) OauthCallback(j *jwt.GinJWTMiddleware) func(*gin.Context) {
	return func(ctx *gin.Context) {
		var params types.OauthPathParams
		if err := ctx.ShouldBindUri(&params); err != nil {
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
			return
		}

		code := ctx.Query("code")
		if code == "" {
			ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": "code is required"})
			return
		}

		user, err := h.Service.OauthCallback(params.OauthName, code)
		if err != nil {
			ctx.Error(err)
			return
		}
		jwtToken, _, err := j.TokenGenerator(user)
		if err != nil {
			ctx.Error(err)
			return
		}
		ctx.SetCookie("jwt", jwtToken, 600, "", "", false, true)
		ctx.Redirect(http.StatusMovedPermanently, "/")
	}
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
// @Router /oauths/{id} [patch]
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
// @Router /oauths/{id} [get]
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
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.Oauth
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /oauths [get]
func (h *Handlers) GetOauths(ctx *gin.Context) {
	var query types.GetOauthsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	oauths, err := h.Service.GetOauths(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	totalCount, err := h.Service.OauthTotalCount(query)
	if err != nil {
		ctx.Error(err)
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(totalCount))
	ctx.JSON(http.StatusOK, oauths)
}
