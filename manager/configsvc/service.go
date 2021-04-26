package configsvc

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

type hostInfo struct {
	objType manager.ObjType
}

type ConfigSvc struct {
	configs *ConfigMap
}

func NewConfigSvc(store Store) *ConfigSvc {
	return &ConfigSvc{
		configs: NewConfigMap(store),
	};
}

func protoConfig2InnerConfig(config *manager.Config) *Config {
	return &Config{
		ID:      config.GetId(),
		Object:  config.GetObject(),
		Type:    config.GetType(),
		Version: config.GetVersion(),
		Data:    config.GetData(),
	}
}

func InnerConfig2ProtoConfig(config *Config) *manager.Config {
	return &manager.Config{
		Id:       config.ID,
		Object:   config.Object,
		Type:     config.Type,
		Version:  config.Version,
		Data:     config.Data,
		CreateAt: config.CreateAt.String(),
		UpdateAt: config.UpdateAt.String(),
	}
}

func (svc *ConfigSvc) AddConfig(ctx context.Context, req *manager.AddConfigRequest) (*manager.AddConfigResponse, error) {
	switch req.Config.GetType() {
	case manager.ObjType_Scheduler.String(), manager.ObjType_Cdn.String():
		if config, err := svc.configs.AddConfig(ctx, NewConfigID(), protoConfig2InnerConfig(req.GetConfig())); err != nil {
			return nil, err
		} else {
			return &manager.AddConfigResponse{
				State: common.NewState(dfcodes.Success, "success"),
				Id:    config.ID,
			}, nil
		}
	default:
		return nil, dferrors.Newf(dfcodes.InvalidObjType, "failed to add Config, req=%+v", req)
	}
}

func (svc *ConfigSvc) DeleteConfig(ctx context.Context, req *manager.DeleteConfigRequest) (*manager.DeleteConfigResponse, error) {
	if _, err := svc.configs.DeleteConfig(ctx, req.GetId()); err != nil {
		return nil, err
	} else {
		return &manager.DeleteConfigResponse{State: common.NewState(dfcodes.Success, "success")}, nil
	}
}

func (svc *ConfigSvc) UpdateConfig(ctx context.Context, req *manager.UpdateConfigRequest) (*manager.UpdateConfigResponse, error) {
	switch req.Config.GetType() {
	case manager.ObjType_Scheduler.String(), manager.ObjType_Cdn.String():
		if _, err := svc.configs.UpdateConfig(ctx, req.GetId(), protoConfig2InnerConfig(req.GetConfig())); err != nil {
			return nil, err
		} else {
			return &manager.UpdateConfigResponse{
				State: common.NewState(dfcodes.Success, "success"),
			}, nil
		}
	default:
		return nil, dferrors.Newf(dfcodes.InvalidObjType, "failed to update Config, req=%+v", req)
	}
}

func (svc *ConfigSvc) GetConfig(ctx context.Context, req *manager.GetConfigRequest) (*manager.GetConfigResponse, error) {
	if config, err := svc.configs.GetConfig(ctx, req.GetId()); err != nil {
		return nil, err
	} else {
		switch config.Type {
		case manager.ObjType_Scheduler.String(), manager.ObjType_Cdn.String():
			return &manager.GetConfigResponse{State: common.NewState(dfcodes.Success, "success"), Config: InnerConfig2ProtoConfig(config)}, nil
		default:
			return nil, dferrors.Newf(dfcodes.InvalidObjType, "failed to get Config, req=%+v", req)
		}
	}
}

func (svc *ConfigSvc) ListConfigs(ctx context.Context, req *manager.ListConfigsRequest) (*manager.ListConfigsResponse, error) {
	configs, err := svc.configs.ListConfigs(ctx, req.GetObject())
	if err != nil {
		return nil, err
	}

	var protoConfigs []*manager.Config
	for _, config := range configs {
		switch config.Type {
		case manager.ObjType_Scheduler.String(), manager.ObjType_Cdn.String():
			protoConfigs = append(protoConfigs, InnerConfig2ProtoConfig(config))
		default:
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to list configs, req=%+v", req)
		}
	}

	return &manager.ListConfigsResponse{
		State:   common.NewState(dfcodes.Success, "success"),
		Configs: protoConfigs,
	}, nil
}

func (svc *ConfigSvc) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*manager.KeepAliveResponse, error) {
	config, err := svc.configs.LatestConfig(ctx, req.GetObject(), req.GetType())
	if err != nil {
		return nil, err
	}

	switch config.Type {
	case manager.ObjType_Scheduler.String(), manager.ObjType_Cdn.String():
		return &manager.KeepAliveResponse{
			State:  common.NewState(dfcodes.Success, "success"),
			Config: InnerConfig2ProtoConfig(config),
		}, nil
	default:
		return nil, dferrors.Newf(dfcodes.ManagerError, "failed to keepalive, req=%+v", req)
	}
}
