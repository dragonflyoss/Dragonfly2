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

// CreateCDNCluster godoc
// @Summary Add cdn cluster
// @Description add by json config
// @Tags CDNCluster
// @Accept  json
// @Produce  json
// @Param cluster body types.CDNCluster true "Cdn cluster"
// @Success 200 {object} types.CDNCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/clusters [post]
func (handler *Handler) CreateCDNCluster(ctx *gin.Context) {
	var cluster types.CDNCluster
	if err := ctx.ShouldBindJSON(&cluster); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	if err := checkCDNClusterValidate(&cluster); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	retCluster, err := handler.server.AddCDNCluster(context.TODO(), &cluster)
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	ctx.JSON(http.StatusOK, retCluster)
}

// DestroyCDNCluster godoc
// @Summary Delete cdn cluster
// @Description Delete by clusterId
// @Tags CDNCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterID"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/clusters/{id} [delete]
func (handler *Handler) DestroyCDNCluster(ctx *gin.Context) {
	var uri types.CDNClusterURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	retCluster, err := handler.server.DeleteCDNCluster(context.TODO(), uri.ClusterID)
	if err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}
	if retCluster != nil {
		ctx.JSON(http.StatusOK, "success")
	} else {
		ctx.Error(NewError(http.StatusNotFound, errors.Newf("cdn cluster not found, id %s", uri.ClusterID)))
	}
}

// UpdateCDNCluster godoc
// @Summary Update cdn cluster
// @Description Update by json cdn cluster
// @Tags CDNCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterID"
// @Param  Cluster body types.CDNCluster true "CDNCluster"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/clusters/{id} [post]
func (handler *Handler) UpdateCDNCluster(ctx *gin.Context) {
	var uri types.CDNClusterURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	var cluster types.CDNCluster
	if err := ctx.ShouldBindJSON(&cluster); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	if err := checkCDNClusterValidate(&cluster); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	cluster.ClusterID = uri.ClusterID
	_, err := handler.server.UpdateCDNCluster(context.TODO(), &cluster)
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	ctx.JSON(http.StatusOK, "success")
}

// GetCDNCluster godoc
// @Summary Get cdn cluster
// @Description Get cdn cluster by ClusterID
// @Tags CDNCluster
// @Accept  json
// @Produce  json
// @Param id path string true "ClusterID"
// @Success 200 {object} types.CDNCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/clusters/{id} [get]
func (handler *Handler) GetCDNCluster(ctx *gin.Context) {
	var uri types.CDNClusterURI
	if err := ctx.ShouldBindUri(&uri); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	retCluster, err := handler.server.GetCDNCluster(context.TODO(), uri.ClusterID)
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	ctx.JSON(http.StatusOK, &retCluster)
}

// ListCDNClusters godoc
// @Summary List cdn clusters
// @Description List by object
// @Tags CDNCluster
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListCDNClustersResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/clusters [get]
func (handler *Handler) ListCDNClusters(ctx *gin.Context) {
	var query types.ListQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.Error(NewError(http.StatusBadRequest, err))
		return
	}

	clusters, err := handler.server.ListCDNClusters(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err != nil {
		ctx.Error(NewError(-1, err))
		return
	}
	if len(clusters) > 0 {
		ctx.JSON(http.StatusOK, &types.ListCDNClustersResponse{Clusters: clusters})
	} else {
		ctx.Error(NewError(http.StatusNotFound, errors.Newf("list cdn clusters empty, marker %d, maxItemCount %d", query.Marker, query.MaxItemCount)))
	}
}

func checkCDNClusterValidate(cluster *types.CDNCluster) (err error) {
	var cdnConfigMap map[string]string
	err = json.Unmarshal([]byte(cluster.Config), &cdnConfigMap)
	if err != nil {
		err = errors.New("unmarshal cdn_config error: cdn_config must map[string]string")
		return
	}

	return
}
