package handler

import (
	"context"
	"d7y.io/dragonfly/v2/manager/apis/v2manager/types"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	proto "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"github.com/gin-gonic/gin"
	"net/http"
)

// AddConfig godoc
// @Summary Add a config
// @Description add by json config
// @Tags configs
// @Accept  json
// @Produce  json
// @Param config body types.Config true "Add config"
// @Success 200 {object} types.AddConfigResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /configs [post]
func (handler *Handler) AddConfig(ctx *gin.Context) {
	var cfg types.Config
	if err := ctx.ShouldBindJSON(cfg); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	req := &proto.AddConfigRequest{
		Config: &proto.Config{
			Object:  cfg.Object,
			Type:    cfg.Type,
			Version: cfg.Version,
			Data:    cfg.Data,
		},
	}

	rep, err := handler.server.AddConfig(context.TODO(), req)
	if err == nil {
		ctx.JSON(http.StatusOK, &types.AddConfigResponse{Id: rep.GetId()})
	} else if dferrors.CheckError(err, dfcodes.InvalidObjType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) || dferrors.CheckError(err, dfcodes.ManagerError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// DeleteConfig godoc
// @Summary Delete a config
// @Description Delete by config ID
// @Tags configs
// @Accept  json
// @Produce  json
// @Param  id path string true "Config ID"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /configs/{id} [delete]
func (handler *Handler) DeleteConfig(ctx *gin.Context) {
	req := &proto.DeleteConfigRequest{
		Id: ctx.Param("id"),
	}

	_, err := handler.server.DeleteConfig(context.TODO(), req)
	if err == nil {
		ctx.JSON(http.StatusOK, "success")
	} else if dferrors.CheckError(err, dfcodes.InvalidObjType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) || dferrors.CheckError(err, dfcodes.ManagerError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// UpdateConfig godoc
// @Summary Update a config
// @Description Update by json config
// @Tags configs
// @Accept  json
// @Produce  json
// @Param  id path string true "Config ID"
// @Param  Config body types.Config true "Update Config"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /configs/{id} [post]
func (handler *Handler) UpdateConfig(ctx *gin.Context) {
	var cfg types.Config
	if err := ctx.ShouldBindJSON(cfg); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	cfg.Id = ctx.Param("id")
	req := &proto.UpdateConfigRequest{
		Id:     cfg.Id,
		Config: typeConfig2protoConfig(cfg),
	}

	_, err := handler.server.UpdateConfig(context.TODO(), req)
	if err == nil {
		ctx.JSON(http.StatusOK, "success")
	} else if dferrors.CheckError(err, dfcodes.InvalidObjType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) || dferrors.CheckError(err, dfcodes.ManagerError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// GetConfig godoc
// @Summary Get a config
// @Description get a config by Config ID
// @Tags configs
// @Accept  json
// @Produce  json
// @Param id path string true "Config ID"
// @Success 200 {object} types.GetConfigResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /configs/{id} [get]
func (handler *Handler) GetConfig(ctx *gin.Context) {
	req := &proto.GetConfigRequest{
		Id: ctx.Param("id"),
	}

	rep, err := handler.server.GetConfig(context.TODO(), req)
	if err == nil {
		ctx.JSON(http.StatusOK, &types.GetConfigResponse{Config: protoConfig2TypeConfig(rep.Config)})
	} else if dferrors.CheckError(err, dfcodes.InvalidObjType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) || dferrors.CheckError(err, dfcodes.ManagerError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

// ListConfigs godoc
// @Summary List configs
// @Description get configs
// @Tags configs
// @Accept  json
// @Produce  json
// @Param object query string true "configs search by object"
// @Success 200 {object} types.ListConfigsResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /configs [get]
func (handler *Handler) ListConfigs(ctx *gin.Context) {
	req := &proto.ListConfigsRequest{
		Object: ctx.Query("object"),
	}

	rep, err := handler.server.ListConfigs(context.TODO(), req)
	if err == nil {
		var configs []*types.Config
		for _, cfg := range rep.Configs {
			configs = append(configs, protoConfig2TypeConfig(cfg))
		}
		ctx.JSON(http.StatusOK, &types.ListConfigsResponse{Configs: configs})
	} else if dferrors.CheckError(err, dfcodes.InvalidObjType) {
		NewError(ctx, http.StatusBadRequest, err)
	} else if dferrors.CheckError(err, dfcodes.ManagerStoreError) || dferrors.CheckError(err, dfcodes.ManagerError) {
		NewError(ctx, http.StatusInternalServerError, err)
	} else {
		NewError(ctx, http.StatusNotFound, err)
	}
}

func protoConfig2TypeConfig(config *proto.Config) *types.Config {
	return &types.Config{
		Id:       config.Id,
		Object:   config.Object,
		Type:     config.Type,
		Version:  config.Version,
		Data:     config.Data,
		CreateAt: config.CreateAt,
		UpdateAt: config.UpdateAt,
	}
}

func typeConfig2protoConfig(config types.Config) *proto.Config {
	return &proto.Config{
		Id:       config.Id,
		Object:   config.Object,
		Type:     config.Type,
		Version:  config.Version,
		Data:     config.Data,
		CreateAt: config.CreateAt,
		UpdateAt: config.UpdateAt,
	}
}
