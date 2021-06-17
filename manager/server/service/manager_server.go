package service

import (
	"context"
	"encoding/json"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/configsvc"
	"d7y.io/dragonfly/v2/manager/dc"
	"d7y.io/dragonfly/v2/manager/host"
	"d7y.io/dragonfly/v2/manager/hostidentifier"
	"d7y.io/dragonfly/v2/manager/lease"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/manager/store/client"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

type ManagerServer struct {
	identifier  hostidentifier.Identifier
	store       store.Store
	hostManager host.Manager
	configSvc   *configsvc.ConfigSvc
	lessor      lease.Lessor
	redisClient *dc.RedisClient
}

func NewManagerServer(cfg *config.Config) (*ManagerServer, error) {
	var err error
	mgr := &ManagerServer{}
	defer func() {
		if err != nil {
			mgr.Close()
		}
	}()

	if err = cfg.Valid(); err != nil {
		return mgr, err
	}

	mgr.identifier = hostidentifier.NewIdentifier()

	mgr.store, err = client.NewStore(cfg)
	if err != nil {
		return mgr, err
	}

	mgr.lessor, err = lease.NewLessor(mgr.store)
	if err != nil {
		return mgr, err
	}

	mgr.redisClient, err = dc.NewRedisClient(cfg.Redis)
	if err != nil {
		return mgr, err
	}

	mgr.configSvc, err = configsvc.NewConfigSvc(mgr.store, mgr.identifier, mgr.lessor, mgr.redisClient)
	if err != nil {
		return mgr, err
	}

	mgr.hostManager, err = host.NewManager(cfg.HostService)
	if err != nil {
		return mgr, err
	}

	return mgr, nil
}

func (ms *ManagerServer) Close() error {
	if ms.configSvc != nil {
		_ = ms.configSvc.Close()
		ms.configSvc = nil
	}

	if ms.redisClient != nil {
		_ = ms.redisClient.Close()
		ms.redisClient = nil
	}

	if ms.lessor != nil {
		_ = ms.lessor.Close()
		ms.lessor = nil
	}

	return nil
}

func (ms *ManagerServer) GetSchedulers(ctx context.Context, req *manager.GetSchedulersRequest) (*manager.SchedulerNodes, error) {
	var opts []host.OpOption
	opts = append(opts, host.WithIP(req.GetIp()))
	opts = append(opts, host.WithHostName(req.GetHostName()))

	hostInfo, err := ms.hostManager.GetHostInfo(ctx, opts...)
	if err != nil {
		logger.Warnf("failed to get host info: %v", err)
		hostInfo = host.NewDefaultHostInfo(req.GetIp(), req.GetHostName())
	}

	nodes, err := ms.configSvc.GetSchedulers(ctx, hostInfo)
	if err != nil {
		return nil, err
	}

	proxyDomain := make(map[string]string)
	if len(hostInfo.SecurityDomain) > 0 {
		if securityDomain, err := ms.configSvc.GetSecurityDomain(ctx, hostInfo.SecurityDomain); err != nil {

		} else if len(securityDomain.ProxyDomain) > 0 {
			if err := json.Unmarshal([]byte(securityDomain.ProxyDomain), &proxyDomain); err != nil {
				return nil, err
			}
		}
	}

	return &manager.SchedulerNodes{
		Addrs: nodes,
		ClientHost: &manager.HostInfo{
			Ip:             hostInfo.IP,
			HostName:       hostInfo.HostName,
			SecurityDomain: hostInfo.SecurityDomain,
			ProxyDomain:    proxyDomain,
			Location:       hostInfo.Location,
			Idc:            hostInfo.IDC,
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
		configsMap := make(map[string]interface{})
		err := json.Unmarshal([]byte(schedulerCluster.Config), &configsMap)
		if err != nil {
			return nil, err
		}

		CDNClusterID, exist := configsMap["CDN_CLUSTER_ID"]
		if !exist {
			return &manager.ClusterConfig{Config: &manager.ClusterConfig_SchedulerConfig{SchedulerConfig: &manager.SchedulerConfig{
				ClusterId:       schedulerCluster.ClusterID,
				ClusterConfig:   schedulerCluster.Config,
				ClientConfig:    schedulerCluster.ClientConfig,
				ClusterVersion:  schedulerCluster.Version,
				InstanceConfig:  schedulerInstance.IDC, // todo InstanceConfig format
				InstanceVersion: schedulerInstance.Version,
				CdnHosts:        []*manager.ServerInfo{},
			}}}, nil
		}

		var cdnInstances []*types.CDNInstance
		maxItemCount := 50
		for marker := 0; ; marker = marker + maxItemCount {
			var opts []store.OpOption
			opts = append(opts, store.WithClusterID(CDNClusterID.(string)))
			opts = append(opts, store.WithMarker(marker, maxItemCount))
			if instances, err := ms.configSvc.ListCDNInstances(context.TODO(), opts...); err != nil {
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

			var opts []host.OpOption
			opts = append(opts, host.WithIP(instance.IP))
			opts = append(opts, host.WithHostName(instance.HostName))
			hostInfo, err := ms.hostManager.GetHostInfo(ctx, opts...)
			if err != nil {
				return nil, dferrors.Newf(dfcodes.ManagerHostError, "get host info error: %v", err)
			}

			proxyDomain := make(map[string]string)
			if len(hostInfo.SecurityDomain) > 0 {
				if securityDomain, err := ms.configSvc.GetSecurityDomain(ctx, hostInfo.SecurityDomain); err != nil {

				} else if len(securityDomain.ProxyDomain) > 0 {
					if err := json.Unmarshal([]byte(securityDomain.ProxyDomain), &proxyDomain); err != nil {
						return nil, err
					}
				}
			}

			cdnHosts = append(cdnHosts, &manager.ServerInfo{
				HostInfo: &manager.HostInfo{
					Ip:             hostInfo.IP,
					HostName:       hostInfo.HostName,
					SecurityDomain: hostInfo.SecurityDomain,
					ProxyDomain:    proxyDomain,
					Location:       hostInfo.Location,
					Idc:            hostInfo.IDC,
					NetTopology:    hostInfo.NetTopology,
				},
				RpcPort:  instance.RPCPort,
				DownPort: instance.DownPort,
			})
		}

		return &manager.ClusterConfig{Config: &manager.ClusterConfig_SchedulerConfig{SchedulerConfig: &manager.SchedulerConfig{
			ClusterId:       schedulerCluster.ClusterID,
			ClusterConfig:   schedulerCluster.Config,
			ClientConfig:    schedulerCluster.ClientConfig,
			ClusterVersion:  schedulerCluster.Version,
			InstanceConfig:  schedulerInstance.IDC, // todo InstanceConfig format
			InstanceVersion: schedulerInstance.Version,
			CdnHosts:        cdnHosts,
		}}}, nil
	} else {
		instance := interInstance.(*types.CDNInstance)
		cluster := interCluster.(*types.CDNCluster)
		return &manager.ClusterConfig{Config: &manager.ClusterConfig_CdnConfig{CdnConfig: &manager.CdnConfig{
			ClusterId:       cluster.ClusterID,
			ClusterConfig:   cluster.Config,
			ClusterVersion:  cluster.Version,
			InstanceConfig:  instance.IDC, // todo InstanceConfig format
			InstanceVersion: instance.Version,
		}}}, nil
	}
}
