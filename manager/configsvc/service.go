package configsvc

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"github.com/golang/protobuf/jsonpb"
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

func (svc *ConfigSvc) AddConfig(ctx context.Context, req *manager.AddConfigRequest) (*manager.AddConfigResponse, error) {
	if manager.ObjType_Scheduler == req.Config.GetObjType() {
		marshal := jsonpb.Marshaler{}
		strConfig, err := marshal.MarshalToString(req.Config.GetSchedulerConfig())
		if err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to add Config, req=%+v", req)
		}

		id := NewConfigID()
		_, err = svc.configs.AddConfig(ctx, id, &Config{
			Object:  req.Config.GetObject(),
			ObjType: req.Config.GetObjType().String(),
			Version: req.Config.GetVersion(),
			Body:    []byte(strConfig),
		})

		if err != nil {
			return nil, err
		} else {
			return &manager.AddConfigResponse{
				State: common.NewState(dfcodes.Success, "success"),
				Id:    id,
			}, nil
		}
	} else if manager.ObjType_Cdn == req.Config.GetObjType() {
		marshal := jsonpb.Marshaler{}
		strConfig, err := marshal.MarshalToString(req.Config.GetCdnConfig())
		if err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to add Config, req=%+v", req)
		}

		id := NewConfigID()
		_, err = svc.configs.AddConfig(ctx, id, &Config{
			Object:  req.Config.GetObject(),
			ObjType: req.Config.GetObjType().String(),
			Version: req.Config.GetVersion(),
			Body:    []byte(strConfig),
		})

		if err != nil {
			return nil, err
		} else {
			return &manager.AddConfigResponse{
				State: common.NewState(dfcodes.Success, "success"),
				Id:    id,
			}, nil
		}
	} else {
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
	if manager.ObjType_Scheduler == req.Config.GetObjType() {
		marshal := jsonpb.Marshaler{}
		config, err := marshal.MarshalToString(req.Config.GetSchedulerConfig())
		if err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to update Config, req=%+v", req)
		}

		_, err = svc.configs.UpdateConfig(ctx, req.GetId(), &Config{
			Object:  req.Config.GetObject(),
			ObjType: req.Config.GetObjType().String(),
			Version: req.Config.GetVersion(),
			Body:    []byte(config),
		})

		if err != nil {
			return nil, err
		} else {
			return &manager.UpdateConfigResponse{
				State: common.NewState(dfcodes.Success, "success"),
			}, nil
		}
	} else if manager.ObjType_Cdn == req.Config.GetObjType() {
		marshal := jsonpb.Marshaler{}
		config, err := marshal.MarshalToString(req.Config.GetCdnConfig())
		if err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to update Config, req=%+v", req)
		}

		_, err = svc.configs.UpdateConfig(ctx, req.GetId(), &Config{
			Object:  req.Config.GetObject(),
			ObjType: req.Config.GetObjType().String(),
			Version: req.Config.GetVersion(),
			Body:    []byte(config),
		})

		if err != nil {
			return nil, err
		} else {
			return &manager.UpdateConfigResponse{
				State: common.NewState(dfcodes.Success, "success"),
			}, nil
		}
	} else {
		return nil, dferrors.Newf(dfcodes.InvalidObjType, "failed to update Config, req=%+v", req)
	}
}

func (svc *ConfigSvc) GetConfig(ctx context.Context, req *manager.GetConfigRequest) (*manager.GetConfigResponse, error) {
	config, err := svc.configs.GetConfig(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	if manager.ObjType_Scheduler.String() == config.ObjType {
		protoConfig := &manager.SchedulerConfig{}
		if err := jsonpb.UnmarshalString(string(config.Body), protoConfig); err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to get Config, req=%+v", req)
		} else {
			return &manager.GetConfigResponse{State: common.NewState(dfcodes.Success, "success"), Config: &manager.Config{
				Object:  config.Object,
				ObjType: manager.ObjType_Scheduler,
				Version: config.Version,
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: protoConfig},
			}}, nil
		}
	} else if manager.ObjType_Cdn.String() == config.ObjType {
		protoConfig := &manager.CdnConfig{}
		if err := jsonpb.UnmarshalString(string(config.Body), protoConfig); err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to get Config, req=%+v", req)
		} else {
			return &manager.GetConfigResponse{State: common.NewState(dfcodes.Success, "success"), Config: &manager.Config{
				Object:  config.Object,
				ObjType: manager.ObjType_Cdn,
				Version: config.Version,
				Body:    &manager.Config_CdnConfig{CdnConfig: protoConfig},
			}}, nil
		}
	} else {
		return nil, dferrors.Newf(dfcodes.InvalidObjType, "failed to get Config, req=%+v", req)
	}
}

func (svc *ConfigSvc) ListConfigs(ctx context.Context, req *manager.ListConfigsRequest) (*manager.ListConfigsResponse, error) {
	configs, err := svc.configs.ListConfigs(ctx, req.GetObject())
	if err != nil {
		return nil, err
	}

	var protoConfigs []*manager.Config

	for _, config := range configs {
		if manager.ObjType_Scheduler.String() == config.ObjType {
			protoConfig := &manager.SchedulerConfig{}
			if err := jsonpb.UnmarshalString(string(config.Body), protoConfig); err != nil {
				return nil, dferrors.Newf(dfcodes.ManagerError, "failed to list configs, req=%+v", req)
			} else {
				protoConfigs = append(protoConfigs, &manager.Config{
					Id:      config.ID,
					Object:  config.Object,
					ObjType: manager.ObjType_Scheduler,
					Version: config.Version,
					Body:    &manager.Config_SchedulerConfig{SchedulerConfig: protoConfig},
				})
			}
		} else {
			protoConfig := &manager.CdnConfig{}
			if err := jsonpb.UnmarshalString(string(config.Body), protoConfig); err != nil {
				return nil, dferrors.Newf(dfcodes.ManagerError, "failed to list configs, req=%+v", req)
			} else {
				protoConfigs = append(protoConfigs, &manager.Config{
					Id:      config.ID,
					Object:  config.Object,
					ObjType: manager.ObjType_Scheduler,
					Version: config.Version,
					Body:    &manager.Config_CdnConfig{CdnConfig: protoConfig},
				})
			}
		}
	}

	if len(protoConfigs) == 0 {
		return &manager.ListConfigsResponse{
			State:   common.NewState(dfcodes.Success, "success"),
			Configs: nil,
		}, nil
	} else {
		return &manager.ListConfigsResponse{
			State:   common.NewState(dfcodes.Success, "success"),
			Configs: protoConfigs,
		}, nil
	}
}

func (svc *ConfigSvc) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*manager.KeepAliveResponse, error) {
	config, err := svc.configs.LatestConfig(ctx, req.GetObject(), req.GetObjType().String())
	if err != nil {
		return nil, err
	}

	if manager.ObjType_Scheduler.String() == config.ObjType {
		protoConfig := &manager.SchedulerConfig{}
		if err := jsonpb.UnmarshalString(string(config.Body), protoConfig); err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to keepalive, req=%+v", req)
		} else {
			return &manager.KeepAliveResponse{
				State: common.NewState(dfcodes.Success, "success"),
				Config: &manager.Config{
					Object:  config.Object,
					ObjType: manager.ObjType_Scheduler,
					Version: config.Version,
					Body:    &manager.Config_SchedulerConfig{SchedulerConfig: protoConfig},
				},
			}, nil
		}
	} else {
		protoConfig := &manager.CdnConfig{}
		if err := jsonpb.UnmarshalString(string(config.Body), protoConfig); err != nil {
			return nil, dferrors.Newf(dfcodes.ManagerError, "failed to keepalive, req=%+v", req)
		} else {
			return &manager.KeepAliveResponse{
				State: common.NewState(dfcodes.Success, "success"),
				Config: &manager.Config{
					Object:  config.Object,
					ObjType: manager.ObjType_Cdn,
					Version: config.Version,
					Body:    &manager.Config_CdnConfig{CdnConfig: protoConfig},
				},
			}, nil
		}
	}
}
