package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
	"gopkg.in/errgo.v2/fmt/errors"
)

// CreateScheduler godoc
// @Summary Add scheduler cluster
// @Description add by json config
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param cluster body types.SchedulerCluster true "Scheduler cluster"
// @Success 200 {object} types.SchedulerCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/clusters [post]
func (handler *Handlers) CreateScheduler(ctx *gin.Context) {
	var json types.CreateSchedulerRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	scheduler, err := handler.server.AddSchedulerCluster(context.TODO(), json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, scheduler)
}

// DestroyScheduler godoc
// @Summary Delete scheduler cluster
// @Description Delete by clusterId
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterID"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/clusters/{id} [delete]
func (handler *Handlers) DestroyScheduler(ctx *gin.Context) {
	var params types.SchedulerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	scheduler, err := handler.server.DeleteSchedulerCluster(context.TODO(), params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, scheduler)
}

// UpdateScheduler godoc
// @Summary Update scheduler cluster
// @Description Update by json scheduler cluster
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterID"
// @Param  Cluster body types.SchedulerCluster true "SchedulerCluster"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/clusters/{id} [post]
func (handler *Handlers) UpdateScheduler(ctx *gin.Context) {
	var params types.SchedulerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.CreateSchedulerRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	scheduler, err := handler.server.UpdateSchedulerCluster(context.TODO(), params.ID, json)
	if err == nil {
		ctx.Error(err)
	}

	ctx.JSON(http.StatusOK, scheduler)
}

// GetScheduler godoc
// @Summary Get scheduler cluster
// @Description Get scheduler cluster by ClusterID
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param id path string true "ClusterID"
// @Success 200 {object} types.SchedulerCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/clusters/{id} [get]
func (handler *Handlers) GetScheduler(ctx *gin.Context) {
	var params types.SchedulerParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	scheduler, err := handler.server.GetSchedulerCluster(context.TODO(), params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, scheduler)
}

// GetSchedulers godoc
// @Summary List scheduler clusters
// @Description List by object
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListSchedulerClustersResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/clusters [get]
func (handler *Handlers) GetSchedulers(ctx *gin.Context) {
	var query types.ListQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	clusters, err := handler.server.ListSchedulerClusters(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err == nil {
		if len(clusters) > 0 {
			ctx.JSON(http.StatusOK, &types.ListSchedulerClustersResponse{Clusters: clusters})
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("list scheduler clusters empty, marker %d, maxItemCount %d", query.Marker, query.MaxItemCount))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

func checkSchedulerValidate(cluster *types.SchedulerCluster) (err error) {
	var configMap map[string]string
	err = json.Unmarshal([]byte(cluster.Config), &configMap)
	if err != nil {
		err = errors.New("unmarshal scheduler_config error: scheduler_config must map[string]string")
		return
	}

	var clientConfigMap map[string]string
	err = json.Unmarshal([]byte(cluster.ClientConfig), &clientConfigMap)
	if err != nil {
		err = errors.New("unmarshal client_config error: client_config must map[string]string")
		return
	}

	return
}
