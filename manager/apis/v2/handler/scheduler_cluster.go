package handler

import (
	"context"
	"encoding/json"
	"net/http"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/store"
	"github.com/gin-gonic/gin"
	"gopkg.in/errgo.v2/fmt/errors"
)

// CreateSchedulerCluster godoc
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
func (handler *Handler) CreateSchedulerCluster(ctx *gin.Context) {
	var cluster types.SchedulerCluster
	if err := ctx.ShouldBindJSON(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkSchedulerClusterValidate(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retCluster, err := handler.server.AddSchedulerCluster(context.TODO(), &cluster)

	if err != nil {
		NewError(ctx, -1, err)
		return
	}
	ctx.JSON(http.StatusOK, retCluster)
}

// DestroySchedulerCluster godoc
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
func (handler *Handler) DestroySchedulerCluster(ctx *gin.Context) {
	var uri types.SchedulerClusterURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retCluster, err := handler.server.DeleteSchedulerCluster(context.TODO(), uri.ClusterID)
	if err != nil {
		NewError(ctx, -1, err)
		return
	}
	if retCluster != nil {
		ctx.JSON(http.StatusOK, "success")
	} else {
		NewError(ctx, http.StatusNotFound, errors.Newf("scheduler cluster not found, id %s", uri.ClusterID))
	}
}

// UpdateSchedulerCluster godoc
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
func (handler *Handler) UpdateSchedulerCluster(ctx *gin.Context) {
	var uri types.SchedulerClusterURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	var cluster types.SchedulerCluster
	if err := ctx.ShouldBindJSON(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkSchedulerClusterValidate(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	cluster.ClusterID = uri.ClusterID
	_, err := handler.server.UpdateSchedulerCluster(context.TODO(), &cluster)
	if err != nil {
		NewError(ctx, -1, err)
		return
	}
	ctx.JSON(http.StatusOK, "success")
}

// GetSchedulerCluster godoc
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
func (handler *Handler) GetSchedulerCluster(ctx *gin.Context) {
	var uri types.SchedulerClusterURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retCluster, err := handler.server.GetSchedulerCluster(context.TODO(), uri.ClusterID)
	if err != nil {
		NewError(ctx, -1, err)
		return
	}
	ctx.JSON(http.StatusOK, &retCluster)
}

// ListSchedulerClusters godoc
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
func (handler *Handler) ListSchedulerClusters(ctx *gin.Context) {
	var query types.ListQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	clusters, err := handler.server.ListSchedulerClusters(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))

	if err != nil {
		NewError(ctx, -1, err)
		return
	}
	if len(clusters) > 0 {
		ctx.JSON(http.StatusOK, &types.ListSchedulerClustersResponse{Clusters: clusters})
	} else {
		NewError(ctx, http.StatusNotFound, errors.Newf("list scheduler clusters empty, marker %d, maxItemCount %d", query.Marker, query.MaxItemCount))
	}
}

func checkSchedulerClusterValidate(cluster *types.SchedulerCluster) (err error) {
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
