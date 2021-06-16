package handlers

import (
	"context"
	"net/http"

	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
	"gopkg.in/errgo.v2/fmt/errors"
)

// CreateCDNInstance godoc
// @Summary Add cdn instance
// @Description add by json config
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param instance body types.CDNInstance true "Cdn instance"
// @Success 200 {object} types.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances [post]
func (handler *Handlers) CreateCDNInstance(ctx *gin.Context) {
	var instance types.CDNInstance
	if err := ctx.ShouldBindJSON(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkCDNInstanceValidate(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retInstance, err := handler.server.AddCDNInstance(context.TODO(), &instance)
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

// DestroyCDNInstance godoc
// @Summary Delete cdn instance
// @Description Delete by instanceId
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceID"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances/{id} [delete]
func (handler *Handlers) DestroyCDNInstance(ctx *gin.Context) {
	var uri types.CDNInstanceURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retInstance, err := handler.server.DeleteCDNInstance(context.TODO(), uri.InstanceID)
	if err == nil {
		if retInstance != nil {
			ctx.JSON(http.StatusOK, "success")
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("cdn instance not found, id %s", uri.InstanceID))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// UpdateCDNInstance godoc
// @Summary Update cdn instance
// @Description Update by json cdn instance
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceID"
// @Param  Instance body types.CDNInstance true "CDNInstance"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances/{id} [post]
func (handler *Handlers) UpdateCDNInstance(ctx *gin.Context) {
	var uri types.CDNInstanceURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	var instance types.CDNInstance
	if err := ctx.ShouldBindJSON(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkCDNInstanceValidate(&instance); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	instance.InstanceID = uri.InstanceID
	_, err := handler.server.UpdateCDNInstance(context.TODO(), &instance)
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

// GetCDNInstance godoc
// @Summary Get cdn instance
// @Description Get cdn instance by InstanceID
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param id path string true "InstanceID"
// @Success 200 {object} types.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances/{id} [get]
func (handler *Handlers) GetCDNInstance(ctx *gin.Context) {
	var uri types.CDNInstanceURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retInstance, err := handler.server.GetCDNInstance(context.TODO(), uri.InstanceID)
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

// GetCDNInstances godoc
// @Summary List cdn instances
// @Description List by object
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListCDNInstancesResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances [get]
func (handler *Handlers) GetCDNInstances(ctx *gin.Context) {
	var query types.ListQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	instances, err := handler.server.ListCDNInstances(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err == nil {
		if len(instances) > 0 {
			ctx.JSON(http.StatusOK, &types.ListCDNInstancesResponse{Instances: instances})
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

func checkCDNInstanceValidate(instance *types.CDNInstance) (err error) {
	if len(instance.ClusterID) <= 0 {
		err = errors.New("cdn clusterId must be set")
		return
	}

	if len(instance.IDC) <= 0 {
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

	if instance.RPCPort == 0 {
		err = errors.New("cdn rpcPort must be set")
		return
	}

	return
}
