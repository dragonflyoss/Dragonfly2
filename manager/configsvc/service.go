package configsvc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/host"
	"d7y.io/dragonfly/v2/manager/hostidentifier"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

var KeepAliveTimeoutMax = 60 * time.Second

const (
	InstanceActive   string = "active"
	InstanceInactive string = "inactive"

	SchedulerClusterPrefix  string = "sclu"
	SchedulerInstancePrefix string = "sins"
	CdnClusterPrefix        string = "cclu"
	CdnInstancePrefix       string = "cins"
)

type schedulerInstance struct {
	instance      *types.SchedulerInstance
	keepAliveTime time.Time
}

type cdnInstance struct {
	instance      *types.CdnInstance
	keepAliveTime time.Time
}

type ConfigSvc struct {
	mu         sync.Mutex
	store      store.Store
	identifier hostidentifier.Identifier

	schClusters    map[string]*types.SchedulerCluster
	cdnClusters    map[string]*types.CdnCluster
	schInstances   map[string]*schedulerInstance
	cdnInstances   map[string]*cdnInstance
	securityDomain map[string]*types.SecurityDomain

	stopC chan struct{}
	wg    sync.WaitGroup
}

func NewConfigSvc(store store.Store, identifier hostidentifier.Identifier) (*ConfigSvc, error) {
	svc := &ConfigSvc{
		store:          store,
		identifier:     identifier,
		schClusters:    make(map[string]*types.SchedulerCluster),
		cdnClusters:    make(map[string]*types.CdnCluster),
		schInstances:   make(map[string]*schedulerInstance),
		cdnInstances:   make(map[string]*cdnInstance),
		securityDomain: make(map[string]*types.SecurityDomain),
		stopC:          make(chan struct{}),
	}

	if err := svc.rebuild(); err != nil {
		return nil, err
	}

	svc.wg.Add(1)
	go svc.checkKeepAliveLoop()
	return svc, nil
}

func (svc *ConfigSvc) checkKeepAliveLoop() {
	defer svc.wg.Done()

	for {
		select {
		case <-svc.stopC:
			return
		case <-time.After(KeepAliveTimeoutMax):
			svc.updateAllInstanceState()
		}
	}
}

func (svc *ConfigSvc) updateAllInstanceState() {
	var schInstances []types.SchedulerInstance
	var cdnInstances []types.CdnInstance

	svc.mu.Lock()
	now := time.Now()
	for _, instance := range svc.schInstances {
		timeout := now.After(instance.keepAliveTime.Add(KeepAliveTimeoutMax))
		if state, ok := instanceNextState(instance.instance.State, timeout); ok {
			inter := *instance.instance
			inter.State = state
			schInstances = append(schInstances, inter)
		}
	}

	for _, instance := range svc.cdnInstances {
		timeout := now.After(instance.keepAliveTime.Add(KeepAliveTimeoutMax))
		if state, ok := instanceNextState(instance.instance.State, timeout); ok {
			inter := *instance.instance
			inter.State = state
			cdnInstances = append(cdnInstances, inter)
		}
	}
	svc.mu.Unlock()

	for _, instance := range schInstances {
		svc.UpdateSchedulerInstance(context.TODO(), &instance)
	}

	for _, instance := range cdnInstances {
		svc.UpdateCdnInstance(context.TODO(), &instance)
	}
}

func (svc *ConfigSvc) rebuild() error {
	maxItemCount := 50
	for marker := 0; ; marker = marker + maxItemCount {
		if clusters, err := svc.ListSchedulerClusters(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(clusters) <= 0 {
			break
		} else {
			for _, cluster := range clusters {
				svc.schClusters[cluster.ClusterId] = cluster
			}
		}
	}

	for marker := 0; ; marker = marker + maxItemCount {
		if clusters, err := svc.ListCdnClusters(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(clusters) <= 0 {
			break
		} else {
			for _, cluster := range clusters {
				svc.cdnClusters[cluster.ClusterId] = cluster
			}
		}
	}

	for marker := 0; ; marker = marker + maxItemCount {
		if instances, err := svc.ListSchedulerInstances(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(instances) <= 0 {
			break
		} else {
			for _, instance := range instances {
				svc.schInstances[instance.InstanceId] = &schedulerInstance{
					instance:      instance,
					keepAliveTime: time.Now(),
				}
				svc.identifier.Put(instance.HostName, instance.InstanceId)
			}
		}
	}

	for marker := 0; ; marker = marker + maxItemCount {
		if instances, err := svc.ListCdnInstances(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(instances) <= 0 {
			break
		} else {
			for _, instance := range instances {
				svc.cdnInstances[instance.InstanceId] = &cdnInstance{
					instance:      instance,
					keepAliveTime: time.Now(),
				}
				svc.identifier.Put(instance.HostName, instance.InstanceId)
			}
		}
	}

	for marker := 0; ; marker = marker + maxItemCount {
		if domains, err := svc.ListSecurityDomains(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(domains) <= 0 {
			break
		} else {
			for _, domain := range domains {
				svc.securityDomain[domain.SecurityDomain] = domain
			}
		}
	}

	return nil
}

func (svc *ConfigSvc) Stop() {
	svc.stopC <- struct{}{}
	svc.wg.Wait()
}

func (svc *ConfigSvc) GetSchedulers(ctx context.Context, hostInfo *host.HostInfo) ([]string, error) {
	nodes := []string{}
	svc.mu.Lock()
	defer svc.mu.Unlock()

	for _, instance := range svc.schInstances {
		if instance.instance.State != InstanceActive {
			continue
		}

		if hostInfo.IsEmpty() {
			nodes = append(nodes, fmt.Sprintf("%s:%d", instance.instance.Ip, instance.instance.Port))
			continue
		}

		if instance.instance.SecurityDomain != hostInfo.SecurityDomain {
			continue
		}

		if instance.instance.Idc == hostInfo.Idc {
			nodes = append(nodes, fmt.Sprintf("%s:%d", instance.instance.Ip, instance.instance.Port))
			continue
		}
	}

	if len(nodes) <= 0 {
		return nil, dferrors.Newf(dfcodes.SchedulerNodesNotFound, "scheduler nodes not found")
	} else {
		return nodes, nil
	}
}

func instanceNextState(cur string, timeout bool) (next string, ok bool) {
	if timeout {
		next = InstanceInactive
	} else {
		next = InstanceActive
	}

	ok = cur != next
	return
}

func (svc *ConfigSvc) KeepAlive(ctx context.Context, req *manager.KeepAliveRequest) error {
	instanceId, exist := svc.identifier.Get(req.GetHostName())
	if !exist {
		return dferrors.Newf(dfcodes.ManagerError, "hostname not exist, %s", req.GetHostName())
	}

	if manager.ResourceType_Scheduler == req.GetType() {
		if instance, exist := svc.getSchedulerInstance(ctx, instanceId); !exist {
			return dferrors.Newf(dfcodes.ManagerError, "Scheduler instance not exist, instanceId %s", instanceId)
		} else {
			now := time.Now()
			timeout := now.After(instance.keepAliveTime.Add(KeepAliveTimeoutMax))
			instance.keepAliveTime = now
			if state, ok := instanceNextState(instance.instance.State, timeout); ok {
				inter := *instance.instance
				inter.State = state
				_, err := svc.UpdateSchedulerInstance(ctx, &inter)
				return err
			} else {
				return nil
			}
		}
	} else if manager.ResourceType_Cdn == req.GetType() {
		if instance, exist := svc.getCdnInstance(ctx, instanceId); !exist {
			return dferrors.Newf(dfcodes.ManagerError, "Cdn instance not exist, instanceId %s", instanceId)
		} else {
			now := time.Now()
			timeout := now.After(instance.keepAliveTime.Add(KeepAliveTimeoutMax))
			instance.keepAliveTime = now
			if state, ok := instanceNextState(instance.instance.State, timeout); ok {
				inter := *instance.instance
				inter.State = state
				_, err := svc.UpdateCdnInstance(ctx, &inter)
				return err
			} else {
				return nil
			}
		}
	} else {
		return dferrors.Newf(dfcodes.InvalidResourceType, "invalid obj type %s", req.GetType())
	}
}

func (svc *ConfigSvc) GetInstanceAndClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest) (interface{}, interface{}, error) {
	instanceId, exist := svc.identifier.Get(req.GetHostName())
	if !exist {
		return nil, nil, dferrors.Newf(dfcodes.ManagerError, "hostname not exist, %s", req.GetHostName())
	}

	if manager.ResourceType_Scheduler == req.GetType() {
		if instance, exist := svc.getSchedulerInstance(ctx, instanceId); !exist {
			return nil, nil, dferrors.Newf(dfcodes.ManagerError, "Scheduler instance not exist, instanceId %s", instanceId)
		} else {
			if cluster, exist := svc.getSchedulerCluster(ctx, instance.instance.ClusterId); !exist {
				return nil, nil, dferrors.Newf(dfcodes.ManagerError, "Scheduler cluster not exist, clusterId %s", instance.instance.ClusterId)
			} else {
				return instance, cluster, nil
			}
		}
	} else if manager.ResourceType_Cdn == req.GetType() {
		if instance, exist := svc.getCdnInstance(ctx, instanceId); !exist {
			return nil, nil, dferrors.Newf(dfcodes.ManagerError, "Cdn instance not exist, instanceId %s", instanceId)
		} else {
			if cluster, exist := svc.getCdnCluster(ctx, instance.instance.ClusterId); !exist {
				return nil, nil, dferrors.Newf(dfcodes.ManagerError, "Cdn cluster not exist, clusterId %s", instance.instance.ClusterId)
			} else {
				return instance, cluster, nil
			}
		}
	} else {
		return nil, nil, dferrors.Newf(dfcodes.InvalidResourceType, "invalid obj type %s", req.GetType())
	}
}

func (svc *ConfigSvc) AddSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster) (*types.SchedulerCluster, error) {
	cluster.ClusterId = NewUUID(SchedulerClusterPrefix)
	if inter, err := svc.store.Add(ctx, cluster.ClusterId, cluster, store.WithResourceType(store.SchedulerCluster)); err != nil {
		return nil, err
	} else {
		cluster = inter.(*types.SchedulerCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		svc.schClusters[cluster.ClusterId] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) DeleteSchedulerCluster(ctx context.Context, clusterId string) (*types.SchedulerCluster, error) {
	if inter, err := svc.store.Delete(ctx, clusterId, store.WithResourceType(store.SchedulerCluster)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		cluster := inter.(*types.SchedulerCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schClusters[cluster.ClusterId]; exist {
			delete(svc.schClusters, cluster.ClusterId)
		}
		return cluster, nil
	}
}

func (svc *ConfigSvc) UpdateSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster) (*types.SchedulerCluster, error) {
	if inter, err := svc.store.Update(ctx, cluster.ClusterId, cluster, store.WithResourceType(store.SchedulerCluster)); err != nil {
		return nil, err
	} else {
		cluster = inter.(*types.SchedulerCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schClusters[cluster.ClusterId]; exist {
			delete(svc.schClusters, cluster.ClusterId)
		}
		svc.schClusters[cluster.ClusterId] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) getSchedulerCluster(ctx context.Context, clusterId string) (*types.SchedulerCluster, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.schClusters[clusterId]; exist {
		return cur, true
	} else {
		return nil, false
	}
}

func (svc *ConfigSvc) GetSchedulerCluster(ctx context.Context, clusterId string) (*types.SchedulerCluster, error) {
	if cluster, exist := svc.getSchedulerCluster(ctx, clusterId); exist {
		return cluster, nil
	} else if inter, err := svc.store.Get(ctx, clusterId, store.WithResourceType(store.SchedulerCluster)); err != nil {
		return nil, err
	} else {
		cluster := inter.(*types.SchedulerCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schClusters[cluster.ClusterId]; exist {
			delete(svc.schClusters, cluster.ClusterId)
		}

		svc.schClusters[cluster.ClusterId] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) ListSchedulerClusters(ctx context.Context, opts ...store.OpOption) ([]*types.SchedulerCluster, error) {
	if inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.SchedulerCluster))...); err != nil {
		return nil, err
	} else {
		clusters := []*types.SchedulerCluster{}
		for _, inner := range inners {
			clusters = append(clusters, inner.(*types.SchedulerCluster))
		}

		return clusters, nil
	}
}

func (svc *ConfigSvc) AddSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance) (*types.SchedulerInstance, error) {
	instance.InstanceId = NewUUID(SchedulerInstancePrefix)
	instance.State = InstanceInactive
	if inter, err := svc.store.Add(ctx, instance.InstanceId, instance, store.WithResourceType(store.SchedulerInstance)); err != nil {
		return nil, err
	} else {
		instance = inter.(*types.SchedulerInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		svc.schInstances[instance.InstanceId] = &schedulerInstance{
			instance:      instance,
			keepAliveTime: time.Now(),
		}

		svc.identifier.Put(instance.HostName, instance.InstanceId)
		return instance, nil
	}
}

func (svc *ConfigSvc) DeleteSchedulerInstance(ctx context.Context, instanceId string) (*types.SchedulerInstance, error) {
	if inter, err := svc.store.Delete(ctx, instanceId, store.WithResourceType(store.SchedulerInstance)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		instance := inter.(*types.SchedulerInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schInstances[instance.InstanceId]; exist {
			delete(svc.schInstances, instance.InstanceId)
		}
		svc.identifier.Delete(instance.HostName)
		return instance, nil
	}
}

func (svc *ConfigSvc) UpdateSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance) (*types.SchedulerInstance, error) {
	if inter, err := svc.store.Update(ctx, instance.InstanceId, instance, store.WithResourceType(store.SchedulerInstance)); err != nil {
		return nil, err
	} else {
		instance = inter.(*types.SchedulerInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schInstances[instance.InstanceId]; exist {
			delete(svc.schInstances, instance.InstanceId)
		}
		svc.schInstances[instance.InstanceId] = &schedulerInstance{
			instance:      instance,
			keepAliveTime: time.Now(),
		}
		return instance, nil
	}
}

func (svc *ConfigSvc) getSchedulerInstance(ctx context.Context, instanceId string) (*schedulerInstance, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.schInstances[instanceId]; exist {
		return cur, true
	} else {
		return nil, false
	}
}

func (svc *ConfigSvc) GetSchedulerInstance(ctx context.Context, instanceId string) (*types.SchedulerInstance, error) {
	if instance, exist := svc.getSchedulerInstance(ctx, instanceId); exist {
		return instance.instance, nil
	} else if inter, err := svc.store.Get(ctx, instanceId, store.WithResourceType(store.SchedulerInstance)); err != nil {
		return nil, err
	} else {
		instance := inter.(*types.SchedulerInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schInstances[instance.InstanceId]; exist {
			delete(svc.schInstances, instance.InstanceId)
		}

		svc.schInstances[instance.InstanceId] = &schedulerInstance{
			instance:      instance,
			keepAliveTime: time.Now(),
		}
		return instance, nil
	}
}

func (svc *ConfigSvc) ListSchedulerInstances(ctx context.Context, opts ...store.OpOption) ([]*types.SchedulerInstance, error) {
	if inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.SchedulerInstance))...); err != nil {
		return nil, err
	} else {
		instances := []*types.SchedulerInstance{}
		for _, inner := range inners {
			instances = append(instances, inner.(*types.SchedulerInstance))
		}

		return instances, nil
	}
}

func (svc *ConfigSvc) AddCdnCluster(ctx context.Context, cluster *types.CdnCluster) (*types.CdnCluster, error) {
	cluster.ClusterId = NewUUID(CdnClusterPrefix)
	if inter, err := svc.store.Add(ctx, cluster.ClusterId, cluster, store.WithResourceType(store.CdnCluster)); err != nil {
		return nil, err
	} else {
		cluster = inter.(*types.CdnCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnClusters[cluster.ClusterId]; exist {
			delete(svc.cdnClusters, cluster.ClusterId)
		}
		svc.cdnClusters[cluster.ClusterId] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) DeleteCdnCluster(ctx context.Context, clusterId string) (*types.CdnCluster, error) {
	if inter, err := svc.store.Delete(ctx, clusterId, store.WithResourceType(store.CdnCluster)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		cluster := inter.(*types.CdnCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnClusters[cluster.ClusterId]; exist {
			delete(svc.cdnClusters, cluster.ClusterId)
		}
		return cluster, nil
	}
}

func (svc *ConfigSvc) UpdateCdnCluster(ctx context.Context, cluster *types.CdnCluster) (*types.CdnCluster, error) {
	if inter, err := svc.store.Update(ctx, cluster.ClusterId, cluster, store.WithResourceType(store.CdnCluster)); err != nil {
		return nil, err
	} else {
		cluster = inter.(*types.CdnCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnClusters[cluster.ClusterId]; exist {
			delete(svc.cdnClusters, cluster.ClusterId)
		}
		svc.cdnClusters[cluster.ClusterId] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) getCdnCluster(ctx context.Context, clusterId string) (*types.CdnCluster, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.cdnClusters[clusterId]; exist {
		return cur, true
	} else {
		return nil, false
	}
}

func (svc *ConfigSvc) GetCdnCluster(ctx context.Context, clusterId string) (*types.CdnCluster, error) {
	if cluster, exist := svc.getCdnCluster(ctx, clusterId); exist {
		return cluster, nil
	} else if inter, err := svc.store.Get(ctx, clusterId, store.WithResourceType(store.CdnCluster)); err != nil {
		return nil, err
	} else {
		cluster := inter.(*types.CdnCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnClusters[cluster.ClusterId]; exist {
			delete(svc.cdnClusters, cluster.ClusterId)
		}

		svc.cdnClusters[cluster.ClusterId] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) ListCdnClusters(ctx context.Context, opts ...store.OpOption) ([]*types.CdnCluster, error) {
	if inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.CdnCluster))...); err != nil {
		return nil, err
	} else {
		clusters := []*types.CdnCluster{}
		for _, inner := range inners {
			clusters = append(clusters, inner.(*types.CdnCluster))
		}

		return clusters, nil
	}
}

func (svc *ConfigSvc) AddCdnInstance(ctx context.Context, instance *types.CdnInstance) (*types.CdnInstance, error) {
	instance.InstanceId = NewUUID(CdnInstancePrefix)
	instance.State = InstanceInactive
	if inter, err := svc.store.Add(ctx, instance.InstanceId, instance, store.WithResourceType(store.CdnInstance)); err != nil {
		return nil, err
	} else {
		instance = inter.(*types.CdnInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnInstances[instance.InstanceId]; exist {
			delete(svc.cdnInstances, instance.InstanceId)
		}
		svc.cdnInstances[instance.InstanceId] = &cdnInstance{
			instance:      instance,
			keepAliveTime: time.Now(),
		}
		svc.identifier.Put(instance.HostName, instance.InstanceId)
		return instance, nil
	}
}

func (svc *ConfigSvc) DeleteCdnInstance(ctx context.Context, instanceId string) (*types.CdnInstance, error) {
	if inter, err := svc.store.Delete(ctx, instanceId, store.WithResourceType(store.CdnInstance)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		instance := inter.(*types.CdnInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnInstances[instance.InstanceId]; exist {
			delete(svc.cdnInstances, instance.InstanceId)
		}
		svc.identifier.Delete(instance.HostName)
		return instance, nil
	}
}

func (svc *ConfigSvc) UpdateCdnInstance(ctx context.Context, instance *types.CdnInstance) (*types.CdnInstance, error) {
	if inter, err := svc.store.Update(ctx, instance.InstanceId, instance, store.WithResourceType(store.CdnInstance)); err != nil {
		return nil, err
	} else {
		instance = inter.(*types.CdnInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnInstances[instance.InstanceId]; exist {
			delete(svc.cdnInstances, instance.InstanceId)
		}
		svc.cdnInstances[instance.InstanceId] = &cdnInstance{
			instance:      instance,
			keepAliveTime: time.Now(),
		}
		return instance, nil
	}
}

func (svc *ConfigSvc) getCdnInstance(ctx context.Context, instanceId string) (*cdnInstance, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.cdnInstances[instanceId]; exist {
		return cur, true
	} else {
		return nil, false
	}
}

func (svc *ConfigSvc) GetCdnInstance(ctx context.Context, instanceId string) (*types.CdnInstance, error) {
	if instance, exist := svc.getCdnInstance(ctx, instanceId); exist {
		return instance.instance, nil
	} else if inter, err := svc.store.Get(ctx, instanceId, store.WithResourceType(store.CdnInstance)); err != nil {
		return nil, err
	} else {
		instance := inter.(*types.CdnInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnInstances[instance.InstanceId]; exist {
			delete(svc.cdnInstances, instance.InstanceId)
		}

		svc.cdnInstances[instance.InstanceId] = &cdnInstance{
			instance:      instance,
			keepAliveTime: time.Now(),
		}
		return instance, nil
	}
}

func (svc *ConfigSvc) ListCdnInstances(ctx context.Context, opts ...store.OpOption) ([]*types.CdnInstance, error) {
	if inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.CdnInstance))...); err != nil {
		return nil, err
	} else {
		instances := []*types.CdnInstance{}
		for _, inner := range inners {
			instances = append(instances, inner.(*types.CdnInstance))
		}

		return instances, nil
	}
}

func (svc *ConfigSvc) AddSecurityDomain(ctx context.Context, securityDomain *types.SecurityDomain) (*types.SecurityDomain, error) {
	if inter, err := svc.store.Add(ctx, securityDomain.SecurityDomain, securityDomain, store.WithResourceType(store.SecurityDomain)); err != nil {
		return nil, err
	} else {
		domain := inter.(*types.SecurityDomain)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		svc.securityDomain[domain.SecurityDomain] = domain
		return domain, nil
	}
}

func (svc *ConfigSvc) DeleteSecurityDomain(ctx context.Context, securityDomain string) (*types.SecurityDomain, error) {
	if inter, err := svc.store.Delete(ctx, securityDomain, store.WithResourceType(store.SecurityDomain)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		domain := inter.(*types.SecurityDomain)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.securityDomain[domain.SecurityDomain]; exist {
			delete(svc.securityDomain, domain.SecurityDomain)
		}
		return domain, nil
	}
}

func (svc *ConfigSvc) UpdateSecurityDomain(ctx context.Context, securityDomain *types.SecurityDomain) (*types.SecurityDomain, error) {
	if inter, err := svc.store.Update(ctx, securityDomain.SecurityDomain, securityDomain, store.WithResourceType(store.SecurityDomain)); err != nil {
		return nil, err
	} else {
		domain := inter.(*types.SecurityDomain)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.securityDomain[domain.SecurityDomain]; exist {
			delete(svc.securityDomain, domain.SecurityDomain)
		}
		svc.securityDomain[domain.SecurityDomain] = domain
		return domain, nil
	}
}
func (svc *ConfigSvc) getSecurityDomain(ctx context.Context, securityDomain string) (*types.SecurityDomain, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.securityDomain[securityDomain]; exist {
		return cur, true
	} else {
		return nil, false
	}
}

func (svc *ConfigSvc) GetSecurityDomain(ctx context.Context, securityDomain string) (*types.SecurityDomain, error) {
	if domain, exist := svc.getSecurityDomain(ctx, securityDomain); exist {
		return domain, nil
	} else if inter, err := svc.store.Get(ctx, securityDomain, store.WithResourceType(store.SecurityDomain)); err != nil {
		return nil, err
	} else {
		domain := inter.(*types.SecurityDomain)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.securityDomain[domain.SecurityDomain]; exist {
			delete(svc.securityDomain, domain.SecurityDomain)
		}
		svc.securityDomain[domain.SecurityDomain] = domain
		return domain, nil
	}
}

func (svc *ConfigSvc) ListSecurityDomains(ctx context.Context, opts ...store.OpOption) ([]*types.SecurityDomain, error) {
	if inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.SecurityDomain))...); err != nil {
		return nil, err
	} else {
		domains := []*types.SecurityDomain{}
		for _, inner := range inners {
			domains = append(domains, inner.(*types.SecurityDomain))
		}

		return domains, nil
	}
}
