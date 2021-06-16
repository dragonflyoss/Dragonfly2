package handlers

import (
	"context"
	"net/http"

	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/manager/types"
	"github.com/gin-gonic/gin"
)

// CreateCDN godoc
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
func (handler *Handlers) CreateCDN(ctx *gin.Context) {
	var json types.CreateCDNRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	retCluster, err := handler.server.AddCDNCluster(context.TODO(), json)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, retCluster)
}

// DestroyCDN godoc
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
func (handler *Handlers) DestroyCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	retCluster, err := handler.server.DeleteCDNCluster(context.TODO(), params.ID)
	if err != nil {
		ctx.Error(err)
		return
	}

	ctx.JSON(http.StatusOK, retCluster)
}

// UpdateCDN godoc
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
func (handler *Handlers) UpdateCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	var json types.UpdateCDNRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.Error(err)
		return
	}

	cdn, err := handler.server.UpdateCDNCluster(context.TODO(), params.ID, json)
	if err != nil {
		ctx.Error(err)
	}

	ctx.JSON(http.StatusOK, cdn)
}

// GetCDN godoc
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
func (handler *Handlers) GetCDN(ctx *gin.Context) {
	var params types.CDNParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.Error(err)
		return
	}

	cdn, err := handler.server.GetCDNCluster(context.TODO(), params.ID)
	if err != nil {
		ctx.Error(err)
	}

	ctx.JSON(http.StatusOK, cdn)
}

// GetCDNs godoc
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
func (handler *Handlers) GetCDNs(ctx *gin.Context) {
	var query types.GetCDNsQuery

	query.Page = 1
	query.PerPage = 10

	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.Error(err)
		return
	}

	cdns, err := handler.server.ListCDNClusters(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err != nil {
		ctx.Error(err)
		return
	}

	// TODO(Gaius) Add pagination link header
	ctx.JSON(http.StatusOK, cdns)
}
