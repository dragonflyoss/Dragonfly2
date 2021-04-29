package handler

import (
	"context"
	"net/http"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
	"gopkg.in/errgo.v2/fmt/errors"
)

// AddCdnInstance godoc
// @Summary Add cdn instance
// @Description add by json config
// @Tags CdnInstance
// @Accept  json
// @Produce  json
// @Param instance body types.CdnInstance true "Cdn instance"
// @Success 200 {object} types.CdnInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdninstances [post]
func (handler *Handler) AddCdnInstance(ctx *gin.Context) {
	var instance types.CdnInstance
	if err := ctx.ShouldBindJSON(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkCdnInstanceValidate(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retInstance, err := handler.server.AddCdnInstance(context.TODO(), &instance)
	if err == nil {
		ctx.JSON(http.StatusOK, retInstance)
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// DeleteCdnInstance godoc
// @Summary Delete cdn instance
// @Description Delete by instanceId
// @Tags CdnInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceId"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdninstances/{id} [delete]
func (handler *Handler) DeleteCdnInstance(ctx *gin.Context) {
	var uri types.CdnInstanceUri
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retInstance, err := handler.server.DeleteCdnInstance(context.TODO(), uri.InstanceId)
	if err == nil {
		if retInstance != nil {
			ctx.JSON(http.StatusOK, "success")
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("cdn instance not found, id %s", uri.InstanceId))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// UpdateCdnInstance godoc
// @Summary Update cdn instance
// @Description Update by json cdn instance
// @Tags CdnInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceId"
// @Param  Instance body types.CdnInstance true "CdnInstance"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdninstances/{id} [post]
func (handler *Handler) UpdateCdnInstance(ctx *gin.Context) {
	var uri types.CdnInstanceUri
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	var instance types.CdnInstance
	if err := ctx.ShouldBindJSON(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkCdnInstanceValidate(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	instance.InstanceId = uri.InstanceId
	_, err := handler.server.UpdateCdnInstance(context.TODO(), &instance)
	if err == nil {
		ctx.JSON(http.StatusOK, "success")
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreNotFound) {
		NewError(ctx, http.StatusNotFound, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// GetCdnInstance godoc
// @Summary Get cdn instance
// @Description Get cdn instance by InstanceId
// @Tags CdnInstance
// @Accept  json
// @Produce  json
// @Param id path string true "InstanceId"
// @Success 200 {object} types.CdnInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdninstances/{id} [get]
func (handler *Handler) GetCdnInstance(ctx *gin.Context) {
	var uri types.CdnInstanceUri
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retInstance, err := handler.server.GetCdnInstance(context.TODO(), uri.InstanceId)
	if err == nil {
		ctx.JSON(http.StatusOK, &retInstance)
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreNotFound) {
		NewError(ctx, http.StatusNotFound, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// ListCdnInstances godoc
// @Summary List cdn instances
// @Description List by object
// @Tags CdnInstance
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListCdnInstancesResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdninstances [get]
func (handler *Handler) ListCdnInstances(ctx *gin.Context) {
	var query types.ListQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	instances, err := handler.server.ListCdnInstances(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err == nil {
		if len(instances) > 0 {
			ctx.JSON(http.StatusOK, &types.ListCdnInstancesResponse{Instances: instances})
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("list cdn instances empty, marker %d, maxItemCount %d", query.Marker, query.MaxItemCount))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

func checkCdnInstanceValidate(instance *types.CdnInstance) (err error) {
	if len(instance.ClusterId) <= 0 {
		err = errors.New("cdn clusterId must be set")
		return
	}

	if len(instance.Idc) <= 0 {
		err = errors.New("cdn idc must be set")
		return
	}

	if instance.Port == 0 {
		err = errors.New("cdn port must be set")
		return
	}

	if instance.DownPort == 0 {
		err = errors.New("cdn downPort must be set")
		return
	}

	if instance.RpcPort == 0 {
		err = errors.New("cdn rpcPort must be set")
		return
	}

	return
}
