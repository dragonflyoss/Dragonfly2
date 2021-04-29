package handler

import (
	"context"
	"encoding/json"
	"net/http"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"github.com/gin-gonic/gin"
	"gopkg.in/errgo.v2/fmt/errors"
)

// AddCdnCluster godoc
// @Summary Add cdn cluster
// @Description add by json config
// @Tags CdnCluster
// @Accept  json
// @Produce  json
// @Param cluster body types.CdnCluster true "Cdn cluster"
// @Success 200 {object} types.CdnCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdnclusters [post]
func (handler *Handler) AddCdnCluster(ctx *gin.Context) {
	var cluster types.CdnCluster
	if err := ctx.ShouldBindJSON(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkCdnClusterValidate(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retCluster, err := handler.server.AddCdnCluster(context.TODO(), &cluster)
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

// DeleteCdnCluster godoc
// @Summary Delete cdn cluster
// @Description Delete by clusterId
// @Tags CdnCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterId"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdnclusters/{id} [delete]
func (handler *Handler) DeleteCdnCluster(ctx *gin.Context) {
	var uri types.CdnClusterUri
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retCluster, err := handler.server.DeleteCdnCluster(context.TODO(), uri.ClusterId)
	if err == nil {
		if retCluster != nil {
			ctx.JSON(http.StatusOK, "success")
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("cdn cluster not found, id %s", uri.ClusterId))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// UpdateCdnCluster godoc
// @Summary Update cdn cluster
// @Description Update by json cdn cluster
// @Tags CdnCluster
// @Accept  json
// @Produce  json
// @Param  id path string true "ClusterId"
// @Param  Cluster body types.CdnCluster true "CdnCluster"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdnclusters/{id} [post]
func (handler *Handler) UpdateCdnCluster(ctx *gin.Context) {
	var uri types.CdnClusterUri
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	var cluster types.CdnCluster
	if err := ctx.ShouldBindJSON(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkCdnClusterValidate(&cluster); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	cluster.ClusterId = uri.ClusterId
	_, err := handler.server.UpdateCdnCluster(context.TODO(), &cluster)
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

// GetCdnCluster godoc
// @Summary Get cdn cluster
// @Description Get cdn cluster by ClusterId
// @Tags CdnCluster
// @Accept  json
// @Produce  json
// @Param id path string true "ClusterId"
// @Success 200 {object} types.CdnCluster
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdnclusters/{id} [get]
func (handler *Handler) GetCdnCluster(ctx *gin.Context) {
	var uri types.CdnClusterUri
	if err := ctx.ShouldBindUri(&uri); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	retCluster, err := handler.server.GetCdnCluster(context.TODO(), uri.ClusterId)
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

// ListCdnClusters godoc
// @Summary List cdn clusters
// @Description List by object
// @Tags CdnCluster
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListCdnClustersResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdnclusters [get]
func (handler *Handler) ListCdnClusters(ctx *gin.Context) {
	var query types.ListQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	clusters, err := handler.server.ListCdnClusters(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
	if err == nil {
		if len(clusters) > 0 {
			ctx.JSON(http.StatusOK, &types.ListCdnClustersResponse{Clusters: clusters})
		} else {
			NewError(ctx, http.StatusNotFound, errors.Newf("list cdn clusters empty, marker %d, maxItemCount %d", query.Marker, query.MaxItemCount))
		}
	} else if dferrors.CheckError(err, dfcodes.InvalidResourceType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

func checkCdnClusterValidate(cluster *types.CdnCluster) (err error) {
	var cdnConfigMap map[string]string
	err = json.Unmarshal([]byte(cluster.Config), &cdnConfigMap)
	if err != nil {
		err = errors.New("unmarshal cdn_config error: cdn_config must map[string]string")
		return
	}

	return
}
