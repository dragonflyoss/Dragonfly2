package server

import (
	"context"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/configsvc"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

type ManagerServer struct {
	configSvc *configsvc.ConfigSvc
	store     configsvc.Store
}

func createConfigStore(cfg *config.Config) (configsvc.Store, error) {
	if cfg.ConfigService.StoreName == "" {
		return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: store-name nil")
	}

	for _, store := range cfg.Stores {
		if cfg.ConfigService.StoreName == store.Name {
			switch store.Type {
			case "memory":
				if store.Memory != nil {
					return configsvc.NewMemoryStore(), nil
				} else {
					return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: memory nil")
				}
			case "mysql":
				if store.Mysql != nil {
					if orm, err := configsvc.NewOrmStore(store); err != nil {
						return nil, err
					} else {
						return orm, nil
					}
				} else {
					return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: mysql nil")
				}
			case "oss":
				return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: oss not support")
			default:
			}
		}
	}

	return nil, dferrors.Newf(dfcodes.ManagerConfigError, "config error: not find store matched")
}

func NewManagerServer(cfg *config.Config) *ManagerServer {
	if err := cfg.CheckValid(); err != nil {
		return nil
	}

	store, err := createConfigStore(cfg)
	if err != nil {
		return nil
	}

	return &ManagerServer{
		configSvc: configsvc.NewConfigSvc(store),
		store:     store,
	}
}

func (ms *ManagerServer) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) (*manager.KeepAliveResponse, error) {
	rep, err := ms.configSvc.KeepAlive(ctx, req)
	return rep, err
}

func (ms *ManagerServer) ListSchedulers(ctx context.Context, req *manager.ListSchedulersRequest) (*manager.ListSchedulersResponse, error) {
	return nil, nil
}

func (ms *ManagerServer) AddConfig(ctx context.Context, req *manager.AddConfigRequest) (*manager.AddConfigResponse, error) {
	rep, err := ms.configSvc.AddConfig(ctx, req)
	return rep, err
}

func (ms *ManagerServer) DeleteConfig(ctx context.Context, req *manager.DeleteConfigRequest) (*manager.DeleteConfigResponse, error) {
	rep, err := ms.configSvc.DeleteConfig(ctx, req)
	return rep, err
}

func (ms *ManagerServer) UpdateConfig(ctx context.Context, req *manager.UpdateConfigRequest) (*manager.UpdateConfigResponse, error) {
	rep, err := ms.configSvc.UpdateConfig(ctx, req)
	return rep, err
}

func (ms *ManagerServer) GetConfig(ctx context.Context, req *manager.GetConfigRequest) (*manager.GetConfigResponse, error) {
	rep, err := ms.configSvc.GetConfig(ctx, req)
	return rep, err
}

func (ms *ManagerServer) ListConfigs(ctx context.Context, req *manager.ListConfigsRequest) (*manager.ListConfigsResponse, error) {
	rep, err := ms.configSvc.ListConfigs(ctx, req)
	return rep, err
}
