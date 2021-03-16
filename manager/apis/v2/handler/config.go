package handler

import (
	"context"
	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	proto "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"errors"
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
	if err := ctx.ShouldBindJSON(&cfg); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkTypeConfigValidate(&cfg); err != nil {
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
	id := ctx.Param("id")
	if id == "" {
		NewError(ctx, http.StatusBadRequest, errors.New("must set id of config you want delete in path of http protocol"))
		return
	}

	req := &proto.DeleteConfigRequest{
		Id: id,
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
	id := ctx.Param("id")
	if id == "" {
		NewError(ctx, http.StatusBadRequest, errors.New("must set id of config you want update in path of http protocol"))
		return
	}

	var cfg types.Config
	if err := ctx.ShouldBindJSON(&cfg); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	if err := checkTypeConfigValidate(&cfg); err != nil {
		NewError(ctx, http.StatusBadRequest, err)
		return
	}

	cfg.Id = id
	req := &proto.UpdateConfigRequest{
		Id:     id,
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
	id := ctx.Param("id")
	if id == "" {
		NewError(ctx, http.StatusBadRequest, errors.New("must set id of config you want get in path of http protocol"))
		return
	}

	req := &proto.GetConfigRequest{
		Id: id,
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
	object := ctx.Query("object")
	if object == "" {
		NewError(ctx, http.StatusBadRequest, errors.New("must set object you want list in query of http protocol"))
		return
	}

	req := &proto.ListConfigsRequest{
		Object: object,
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

func checkTypeConfigValidate(config *types.Config) (err error) {
	if config.Object == "" {
		err = errors.New("object in config must be set")
		return
	}

	if config.Type != proto.ObjType_Scheduler.String() && config.Type != proto.ObjType_Cdn.String() {
		err = errors.New(`type in config must be one of "Cdn" or "Scheduler"`)
		return
	}

	if config.Version == 0 {
		err = errors.New("version in config must be not-zero")
		return
	}

	if len(config.Data) <= 0 {
		err = errors.New("data in config must be set")
	}

	return nil
}
