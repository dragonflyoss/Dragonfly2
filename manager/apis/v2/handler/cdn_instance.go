package handler

import (
	"context"
	"net/http"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/store"
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
func (handler *Handler) CreateCDNInstance(ctx *gin.Context) {
	var instance types.CDNInstance
	if err := ctx.ShouldBindJSON(&instance); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	if err := checkCDNInstanceValidate(&instance); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	retInstance, err := handler.server.AddCDNInstance(context.TODO(), &instance)
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	ctx.JSON(http.StatusOK, retInstance)
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
func (handler *Handler) DestroyCDNInstance(ctx *gin.Context) {
	var uri types.CDNInstanceURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	retInstance, err := handler.server.DeleteCDNInstance(context.TODO(), uri.InstanceID)
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	if retInstance != nil {
		ctx.JSON(http.StatusOK, "success")
	} else {
		ctx.Error(NewError(http.StatusNotFound, errors.Newf("cdn instance not found, id %s", uri.InstanceID)))
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
func (handler *Handler) UpdateCDNInstance(ctx *gin.Context) {
	var uri types.CDNInstanceURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	var instance types.CDNInstance
	if err := ctx.ShouldBindJSON(&instance); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	if err := checkCDNInstanceValidate(&instance); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	instance.InstanceID = uri.InstanceID
	_, err := handler.server.UpdateCDNInstance(context.TODO(), &instance)
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	ctx.JSON(http.StatusOK, "success")
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
func (handler *Handler) GetCDNInstance(ctx *gin.Context) {
	var uri types.CDNInstanceURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	retInstance, err := handler.server.GetCDNInstance(context.TODO(), uri.InstanceID)
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	ctx.JSON(http.StatusOK, &retInstance)
}

// ListCDNInstances godoc
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
func (handler *Handler) ListCDNInstances(ctx *gin.Context) {
	var query types.ListQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	instances, err := handler.server.ListCDNInstances(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	if len(instances) > 0 {
		ctx.JSON(http.StatusOK, &types.ListCDNInstancesResponse{Instances: instances})
	} else {
		ctx.Error(NewError(http.StatusNotFound, errors.Newf("list cdn instances empty, marker %d, maxItemCount %d", query.Marker, query.MaxItemCount)))
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
