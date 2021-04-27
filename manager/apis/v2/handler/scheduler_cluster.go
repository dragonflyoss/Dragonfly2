package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/configsvc"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
	"gopkg.in/errgo.v2/fmt/errors"
)

// AddSchedulerCluster godoc
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
// @Router /schedulerclusters [post]
func (handler *Handler) AddSchedulerCluster(ctx *gin.Context) {
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
	if err == nil {
		ctx.JSON(http.StatusOK, retCluster)
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// DeleteSchedulerCluster godoc
// @Summary Delete scheduler cluster
// @Description Delete by clusterId
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterId"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulerclusters/{id} [delete]
func (handler *Handler) DeleteSchedulerCluster(ctx *gin.Context) {
	id := ctx.Param("id")
	if id == "" {
		NewError(ctx, http.StatusBadRequest, errors.New("must set clusterId you want delete in path of http protocol"))
		return
	}

	retCluster, err := handler.server.DeleteSchedulerCluster(context.TODO(), id)
	if err == nil {
		if retCluster != nil {
			ctx.JSON(http.StatusOK, "success")
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("scheduler cluster not found, id %s", id))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// UpdateSchedulerCluster godoc
// @Summary Update scheduler cluster
// @Description Update by json scheduler cluster
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterId"
// @Param  Cluster body types.SchedulerCluster true "SchedulerCluster"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulerclusters/{id} [post]
func (handler *Handler) UpdateSchedulerCluster(ctx *gin.Context) {
	id := ctx.Param("id")
	if id == "" {
		NewError(ctx, http.StatusBadRequest, errors.New("must set clusterId you want update in path of http protocol"))
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

	cluster.ClusterId = id
	_, err := handler.server.UpdateSchedulerCluster(context.TODO(), &cluster)
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

// GetSchedulerCluster godoc
// @Summary Get scheduler cluster
// @Description Get scheduler cluster by ClusterId
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param id path string true "ClusterId"
// @Success 200 {object} types.SchedulerCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulerclusters/{id} [get]
func (handler *Handler) GetSchedulerCluster(ctx *gin.Context) {
	id := ctx.Param("id")
	if id == "" {
		NewError(ctx, http.StatusBadRequest, errors.New("must set clusterId you want get in path of http protocol"))
		return
	}

	retCluster, err := handler.server.GetSchedulerCluster(context.TODO(), id)
	if err == nil {
		ctx.JSON(http.StatusOK, &retCluster)
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

// ListSchedulerClusters godoc
// @Summary List scheduler clusters
// @Description List by object
// @Tags SchedulerCluster
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page"
// @Param maxItemCount query int true "return max item count, default 10, max 50"
// @Success 200 {object} types.ListSchedulerClustersResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /schedulerclusters [get]
func (handler *Handler) ListSchedulerClusters(ctx *gin.Context) {
	var maxItemCount int
	var marker int
	var err error

	qMaxItemCount := ctx.Query("maxItemCount")
	if len(qMaxItemCount) > 0 {
		maxItemCount, err = strconv.Atoi(qMaxItemCount)
		if err != nil {
			NewError(ctx, http.StatusBadRequest, err)
			return
		}

		if maxItemCount > 50 {
			maxItemCount = 50
		}
	} else {
		maxItemCount = 10
	}

	qMarker := ctx.Query("marker")
	if len(qMarker) > 0 {
		marker, err = strconv.Atoi(qMarker)
		if err != nil {
			NewError(ctx, http.StatusBadRequest, err)
			return
		}
	} else {
		marker = 0
	}

	clusters, err := handler.server.ListSchedulerClusters(context.TODO(), configsvc.WithMarker(marker, maxItemCount))
	if err == nil {
		if len(clusters) > 0 {
			ctx.JSON(http.StatusOK, &types.ListSchedulerClustersResponse{Clusters: clusters})
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("list scheduler clusters empty, marker %d, maxItemCount %d", qMarker, qMaxItemCount))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

func checkSchedulerClusterValidate(cluster *types.SchedulerCluster) (err error) {
	if len(cluster.SchedulerConfig) <= 0 {
		err = errors.New("scheduler config must be set")
		return
	}

	if len(cluster.ClientConfig) <= 0 {
		err = errors.New("client config must be set")
		return
	}

	var schedulerConfigMap map[string]string
	err = json.Unmarshal([]byte(cluster.SchedulerConfig), &schedulerConfigMap)
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
