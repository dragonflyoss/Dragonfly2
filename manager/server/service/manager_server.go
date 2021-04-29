package service

import (
	"context"
	"encoding/json"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/configsvc"
	"d7y.io/dragonfly/v2/manager/host"
	"d7y.io/dragonfly/v2/manager/hostidentifier"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

type ManagerServer struct {
	identifier  hostidentifier.Identifier
	store       store.Store
	hostManager host.HostManager
	configSvc   *configsvc.ConfigSvc
}

func NewManagerServer(cfg *config.Config) *ManagerServer {
	if err := cfg.CheckValid(); err != nil {
		return nil
	}

	identifier := hostidentifier.NewIdentifier()

	store, err := store.NewStore(cfg)
	if err != nil {
		return nil
	}

	configSvc, err := configsvc.NewConfigSvc(store, identifier)
	if err != nil {
		return nil
	}

	hostManager, err := host.NewHostManager(cfg.HostService)
	if err != nil {
		return nil
	}

	return &ManagerServer{
		identifier:  identifier,
		store:       store,
		hostManager: hostManager,
		configSvc:   configSvc,
	}
}

func (ms *ManagerServer) GetSchedulers(ctx context.Context, req *manager.GetSchedulersRequest) (*manager.SchedulerNodes, error) {
	hostInfo, err := ms.hostManager.GetHostInfo("", req.GetIp(), req.GetHostName(), "")
	if err != nil {
		logger.Warnf("failed to get host info: %v", err)
		hostInfo = host.NewEmptyHostInfo(req.GetIp(), req.GetHostName())
	}

	nodes, err := ms.configSvc.GetSchedulers(ctx, hostInfo)
	if err != nil {
		return nil, err
	}

	proxyDomain := make(map[string]string)
	if len(hostInfo.SecurityDomain) > 0 {
		if securityDomain, err := ms.configSvc.GetSecurityDomain(ctx, hostInfo.SecurityDomain); err != nil {
			;
		} else if len(securityDomain.ProxyDomain) > 0 {
			if err := json.Unmarshal([]byte(securityDomain.ProxyDomain), &proxyDomain); err != nil {
				return nil, err
			}
		}
	}

	return &manager.SchedulerNodes{
		Addrs: nodes,
		ClientHost: &manager.HostInfo{
			Ip:             hostInfo.Ip,
			HostName:       hostInfo.HostName,
			SecurityDomain: hostInfo.SecurityDomain,
			ProxyDomain:    proxyDomain,
			Location:       hostInfo.Location,
			Idc:            hostInfo.Idc,
			NetTopology:    hostInfo.NetTopology,
		},
	}, nil
}

func (ms *ManagerServer) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) error {
	err := ms.configSvc.KeepAlive(ctx, req)
	return err
}

func (ms *ManagerServer) GetClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest) (*manager.ClusterConfig, error) {
	if interInstance, interCluster, err := ms.configSvc.GetInstanceAndClusterConfig(ctx, req); err != nil {
		return nil, err
	} else if manager.ResourceType_Scheduler == req.GetType() {
		schedulerInstance := interInstance.(*types.SchedulerInstance)
		schedulerCluster := interCluster.(*types.SchedulerCluster)
		schedulerConfigsMap := make(map[string]interface{})
		err := json.Unmarshal([]byte(schedulerCluster.SchedulerConfig), &schedulerConfigsMap)
		if err != nil {
			return nil, err
		}

		cdnClusterId, exist := schedulerConfigsMap["CDN_CLUSTER_ID"]
		if !exist {
			return &manager.ClusterConfig{Config: &manager.ClusterConfig_SchedulerConfig{SchedulerConfig: &manager.SchedulerConfig{
				ClusterId:       schedulerCluster.ClusterId,
				ClusterConfig:   schedulerCluster.SchedulerConfig,
				ClientConfig:    schedulerCluster.ClientConfig,
				ClusterVersion:  schedulerCluster.Version,
				InstanceConfig:  schedulerInstance.Idc, // todo InstanceConfig format
				InstanceVersion: schedulerInstance.Version,
				CdnHosts:        []*manager.ServerInfo{},
			}}}, nil
		}

		var cdnInstances []*types.CdnInstance
		maxItemCount := 50
		for marker := 0; ; marker = marker + maxItemCount {
			var opts []store.OpOption
			opts = append(opts, store.WithClusterId(cdnClusterId.(string)))
			opts = append(opts, store.WithMarker(marker, maxItemCount))
			if instances, err := ms.configSvc.ListCdnInstances(context.TODO(), opts...); err != nil {
				return nil, err
			} else if len(instances) <= 0 {
				break
			} else {
				cdnInstances = append(cdnInstances, instances...)
			}
		}

		var cdnHosts []*manager.ServerInfo
		for _, instance := range cdnInstances {
			if instance.State != configsvc.InstanceActive {
				continue
			}

			hostInfo, err := ms.hostManager.GetHostInfo("", instance.Ip, instance.HostName, "")
			if err != nil {
				return nil, dferrors.Newf(dfcodes.ManagerHostError, "get host info error: %v", err)
			}

			proxyDomain := make(map[string]string)
			if len(hostInfo.SecurityDomain) > 0 {
				if securityDomain, err := ms.configSvc.GetSecurityDomain(ctx, hostInfo.SecurityDomain); err != nil {
					;
				} else if len(securityDomain.ProxyDomain) > 0 {
					if err := json.Unmarshal([]byte(securityDomain.ProxyDomain), &proxyDomain); err != nil {
						return nil, err
					}
				}
			}

			cdnHosts = append(cdnHosts, &manager.ServerInfo{
				HostInfo: &manager.HostInfo{
					Ip:             hostInfo.Ip,
					HostName:       hostInfo.HostName,
					SecurityDomain: hostInfo.SecurityDomain,
					ProxyDomain:    proxyDomain,
					Location:       hostInfo.Location,
					Idc:            hostInfo.Idc,
					NetTopology:    hostInfo.NetTopology,
				},
				RpcPort:  instance.RpcPort,
				DownPort: instance.DownPort,
			})
		}

		return &manager.ClusterConfig{Config: &manager.ClusterConfig_SchedulerConfig{SchedulerConfig: &manager.SchedulerConfig{
			ClusterId:       schedulerCluster.ClusterId,
			ClusterConfig:   schedulerCluster.SchedulerConfig,
			ClientConfig:    schedulerCluster.ClientConfig,
			ClusterVersion:  schedulerCluster.Version,
			InstanceConfig:  schedulerInstance.Idc, // todo InstanceConfig format
			InstanceVersion: schedulerInstance.Version,
			CdnHosts:        cdnHosts,
		}}}, nil
	} else {
		instance := interInstance.(*types.CdnInstance)
		cluster := interCluster.(*types.CdnCluster)
		return &manager.ClusterConfig{Config: &manager.ClusterConfig_CdnConfig{CdnConfig: &manager.CdnConfig{
			ClusterId:       cluster.ClusterId,
			ClusterConfig:   cluster.Config,
			ClusterVersion:  cluster.Version,
			InstanceConfig:  instance.Idc, // todo InstanceConfig format
			InstanceVersion: instance.Version,
		}}}, nil
	}
}

func (ms *ManagerServer) AddSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster) (*types.SchedulerCluster, error) {
	return ms.configSvc.AddSchedulerCluster(ctx, cluster)
}

func (ms *ManagerServer) DeleteSchedulerCluster(ctx context.Context, clusterId string) (*types.SchedulerCluster, error) {
	return ms.configSvc.DeleteSchedulerCluster(ctx, clusterId)
}

func (ms *ManagerServer) UpdateSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster) (*types.SchedulerCluster, error) {
	return ms.configSvc.UpdateSchedulerCluster(ctx, cluster)
}

func (ms *ManagerServer) GetSchedulerCluster(ctx context.Context, clusterId string) (*types.SchedulerCluster, error) {
	return ms.configSvc.GetSchedulerCluster(ctx, clusterId)
}

func (ms *ManagerServer) ListSchedulerClusters(ctx context.Context, opts ...store.OpOption) ([]*types.SchedulerCluster, error) {
	return ms.configSvc.ListSchedulerClusters(ctx, opts...)
}

func (ms *ManagerServer) AddSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance) (*types.SchedulerInstance, error) {
	return ms.configSvc.AddSchedulerInstance(ctx, instance)
}

func (ms *ManagerServer) DeleteSchedulerInstance(ctx context.Context, instanceId string) (*types.SchedulerInstance, error) {
	return ms.configSvc.DeleteSchedulerInstance(ctx, instanceId)
}

func (ms *ManagerServer) UpdateSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance) (*types.SchedulerInstance, error) {
	return ms.configSvc.UpdateSchedulerInstance(ctx, instance)
}

func (ms *ManagerServer) GetSchedulerInstance(ctx context.Context, instanceId string) (*types.SchedulerInstance, error) {
	return ms.configSvc.GetSchedulerInstance(ctx, instanceId)
}

func (ms *ManagerServer) ListSchedulerInstances(ctx context.Context, opts ...store.OpOption) ([]*types.SchedulerInstance, error) {
	return ms.configSvc.ListSchedulerInstances(ctx, opts...)
}

func (ms *ManagerServer) AddCdnCluster(ctx context.Context, cluster *types.CdnCluster) (*types.CdnCluster, error) {
	return ms.configSvc.AddCdnCluster(ctx, cluster)
}

func (ms *ManagerServer) DeleteCdnCluster(ctx context.Context, clusterId string) (*types.CdnCluster, error) {
	return ms.configSvc.DeleteCdnCluster(ctx, clusterId)
}

func (ms *ManagerServer) UpdateCdnCluster(ctx context.Context, cluster *types.CdnCluster) (*types.CdnCluster, error) {
	return ms.configSvc.UpdateCdnCluster(ctx, cluster)
}

func (ms *ManagerServer) GetCdnCluster(ctx context.Context, clusterId string) (*types.CdnCluster, error) {
	return ms.configSvc.GetCdnCluster(ctx, clusterId)
}

func (ms *ManagerServer) ListCdnClusters(ctx context.Context, opts ...store.OpOption) ([]*types.CdnCluster, error) {
	return ms.configSvc.ListCdnClusters(ctx, opts...)
}

func (ms *ManagerServer) AddCdnInstance(ctx context.Context, instance *types.CdnInstance) (*types.CdnInstance, error) {
	return ms.configSvc.AddCdnInstance(ctx, instance)
}

func (ms *ManagerServer) DeleteCdnInstance(ctx context.Context, instanceId string) (*types.CdnInstance, error) {
	return ms.configSvc.DeleteCdnInstance(ctx, instanceId)
}

func (ms *ManagerServer) UpdateCdnInstance(ctx context.Context, instance *types.CdnInstance) (*types.CdnInstance, error) {
	return ms.configSvc.UpdateCdnInstance(ctx, instance)
}

func (ms *ManagerServer) GetCdnInstance(ctx context.Context, instanceId string) (*types.CdnInstance, error) {
	return ms.configSvc.GetCdnInstance(ctx, instanceId)
}

func (ms *ManagerServer) ListCdnInstances(ctx context.Context, opts ...store.OpOption) ([]*types.CdnInstance, error) {
	return ms.configSvc.ListCdnInstances(ctx, opts...)
}

func (ms *ManagerServer) AddSecurityDomain(ctx context.Context, securityDomain *types.SecurityDomain) (*types.SecurityDomain, error) {
	return ms.configSvc.AddSecurityDomain(ctx, securityDomain)
}

func (ms *ManagerServer) DeleteSecurityDomain(ctx context.Context, securityDomain string) (*types.SecurityDomain, error) {
	return ms.configSvc.DeleteSecurityDomain(ctx, securityDomain)
}

func (ms *ManagerServer) UpdateSecurityDomain(ctx context.Context, securityDomain *types.SecurityDomain) (*types.SecurityDomain, error) {
	return ms.configSvc.UpdateSecurityDomain(ctx, securityDomain)
}

func (ms *ManagerServer) GetSecurityDomain(ctx context.Context, securityDomain string) (*types.SecurityDomain, error) {
	return ms.configSvc.GetSecurityDomain(ctx, securityDomain)
}

func (ms *ManagerServer) ListSecurityDomains(ctx context.Context, opts ...store.OpOption) ([]*types.SecurityDomain, error) {
	return ms.configSvc.ListSecurityDomains(ctx, opts...)
}
