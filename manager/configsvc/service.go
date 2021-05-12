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
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
)

var KeepAliveTimeoutMax = 60 * time.Second

const (
	InstanceActive   string = "active"
	InstanceInactive string = "inactive"

	SchedulerClusterPrefix  string = "sclu-"
	SchedulerInstancePrefix string = "sins-"
	CDNClusterPrefix        string = "cclu-"
	CDNInstancePrefix       string = "cins-"
)

type schedulerInstance struct {
	instance      *types.SchedulerInstance
	keepAliveTime time.Time
}

type cdnInstance struct {
	instance      *types.CDNInstance
	keepAliveTime time.Time
}

type ConfigSvc struct {
	mu         sync.Mutex
	store      store.Store
	identifier hostidentifier.Identifier

	schClusters    map[string]*types.SchedulerCluster
	cdnClusters    map[string]*types.CDNCluster
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
		cdnClusters:    make(map[string]*types.CDNCluster),
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
	var cdnInstances []types.CDNInstance

	svc.mu.Lock()
	now := time.Now()
	for _, instance := range svc.schInstances {
		if now.Before(instance.keepAliveTime.Add(KeepAliveTimeoutMax)) {
			continue
		}

		if state, ok := instanceNextState(instance.instance.State, true); ok {
			inter := *instance.instance
			inter.State = state
			schInstances = append(schInstances, inter)
		}
	}

	for _, instance := range svc.cdnInstances {
		if now.Before(instance.keepAliveTime.Add(KeepAliveTimeoutMax)) {
			continue
		}

		if state, ok := instanceNextState(instance.instance.State, true); ok {
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
		svc.UpdateCDNInstance(context.TODO(), &instance)
	}
}

//gocyclo:ignore
func (svc *ConfigSvc) rebuild() error {
	maxItemCount := 50
	for marker := 0; ; marker = marker + maxItemCount {
		if clusters, err := svc.ListSchedulerClusters(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(clusters) <= 0 {
			break
		} else {
			for _, cluster := range clusters {
				svc.schClusters[cluster.ClusterID] = cluster
			}
		}
	}

	for marker := 0; ; marker = marker + maxItemCount {
		if clusters, err := svc.ListCDNClusters(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(clusters) <= 0 {
			break
		} else {
			for _, cluster := range clusters {
				svc.cdnClusters[cluster.ClusterID] = cluster
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
				svc.schInstances[instance.InstanceID] = &schedulerInstance{
					instance:      instance,
					keepAliveTime: time.Now(),
				}
				svc.identifier.Put(SchedulerInstancePrefix+instance.HostName, instance.InstanceID)
			}
		}
	}

	for marker := 0; ; marker = marker + maxItemCount {
		if instances, err := svc.ListCDNInstances(context.TODO(), store.WithMarker(marker, maxItemCount)); err != nil {
			return err
		} else if len(instances) <= 0 {
			break
		} else {
			for _, instance := range instances {
				svc.cdnInstances[instance.InstanceID] = &cdnInstance{
					instance:      instance,
					keepAliveTime: time.Now(),
				}
				svc.identifier.Put(CDNInstancePrefix+instance.HostName, instance.InstanceID)
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

func (svc *ConfigSvc) GetSchedulers(ctx context.Context, hostInfo *host.Info) ([]string, error) {
	nodes := []string{}
	svc.mu.Lock()
	defer svc.mu.Unlock()

	for _, instance := range svc.schInstances {
		if instance.instance.State != InstanceActive {
			continue
		}

		if hostInfo.IsDefault() {
			nodes = append(nodes, fmt.Sprintf("%s:%d", instance.instance.IP, instance.instance.Port))
			continue
		}

		if instance.instance.SecurityDomain != hostInfo.SecurityDomain {
			continue
		}

		if instance.instance.IDC == hostInfo.IDC {
			nodes = append(nodes, fmt.Sprintf("%s:%d", instance.instance.IP, instance.instance.Port))
			continue
		}
	}

	if len(nodes) <= 0 {
		return nil, dferrors.Newf(dfcodes.SchedulerNodesNotFound, "scheduler nodes not found")
	}

	return nodes, nil
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
	logger.Debugf("receive heart beat from %s, type %s", req.HostName, req.Type)
	if manager.ResourceType_Scheduler == req.GetType() {
		instanceID, exist := svc.identifier.Get(SchedulerInstancePrefix + req.GetHostName())
		if !exist {
			return dferrors.Newf(dfcodes.ManagerError, "hostname not exist, %s", req.GetHostName())
		}

		instance, exist := svc.getSchedulerInstance(ctx, instanceID)
		if !exist {
			return dferrors.Newf(dfcodes.ManagerError, "Scheduler instance not exist, instanceID %s", instanceID)
		}

		instance.keepAliveTime = time.Now()
		if state, ok := instanceNextState(instance.instance.State, false); ok {
			inter := *instance.instance
			inter.State = state
			_, err := svc.UpdateSchedulerInstance(ctx, &inter)
			return err
		}

		return nil
	} else if manager.ResourceType_Cdn == req.GetType() {
		instanceID, exist := svc.identifier.Get(CDNInstancePrefix + req.GetHostName())
		if !exist {
			return dferrors.Newf(dfcodes.ManagerError, "hostname not exist, %s", req.GetHostName())
		}

		instance, exist := svc.getCDNInstance(ctx, instanceID)
		if !exist {
			return dferrors.Newf(dfcodes.ManagerError, "Cdn instance not exist, instanceID %s", instanceID)
		}

		instance.keepAliveTime = time.Now()
		if state, ok := instanceNextState(instance.instance.State, false); ok {
			inter := *instance.instance
			inter.State = state
			_, err := svc.UpdateCDNInstance(ctx, &inter)
			return err
		}

		return nil
	} else {
		return dferrors.Newf(dfcodes.InvalidResourceType, "invalid obj type %s", req.GetType())
	}
}

func (svc *ConfigSvc) GetInstanceAndClusterConfig(ctx context.Context, req *manager.GetClusterConfigRequest) (interface{}, interface{}, error) {
	if manager.ResourceType_Scheduler == req.GetType() {
		instanceID, exist := svc.identifier.Get(SchedulerInstancePrefix + req.GetHostName())
		if !exist {
			return nil, nil, dferrors.Newf(dfcodes.ManagerError, "hostname not exist, %s", req.GetHostName())
		}

		instance, err := svc.GetSchedulerInstance(ctx, instanceID)
		if err != nil {
			return nil, nil, err
		}

		cluster, err := svc.GetSchedulerCluster(ctx, instance.ClusterID)
		if err != nil {
			return nil, nil, err
		}

		return instance, cluster, nil
	} else if manager.ResourceType_Cdn == req.GetType() {
		instanceID, exist := svc.identifier.Get(CDNInstancePrefix + req.GetHostName())
		if !exist {
			return nil, nil, dferrors.Newf(dfcodes.ManagerError, "hostname not exist, %s", req.GetHostName())
		}

		instance, err := svc.GetCDNInstance(ctx, instanceID)
		if err != nil {
			return nil, nil, err
		}

		cluster, err := svc.GetCDNCluster(ctx, instance.ClusterID)
		if err != nil {
			return nil, nil, err
		}

		return instance, cluster, nil
	} else {
		return nil, nil, dferrors.Newf(dfcodes.InvalidResourceType, "invalid obj type %s", req.GetType())
	}
}

func (svc *ConfigSvc) AddSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster) (*types.SchedulerCluster, error) {
	cluster.ClusterID = NewUUID(SchedulerClusterPrefix)
	inter, err := svc.store.Add(ctx, cluster.ClusterID, cluster, store.WithResourceType(store.SchedulerCluster))
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.SchedulerCluster)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	svc.schClusters[cluster.ClusterID] = cluster
	return cluster, nil
}

func (svc *ConfigSvc) DeleteSchedulerCluster(ctx context.Context, clusterID string) (*types.SchedulerCluster, error) {
	if inter, err := svc.store.Delete(ctx, clusterID, store.WithResourceType(store.SchedulerCluster)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		cluster := inter.(*types.SchedulerCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schClusters[cluster.ClusterID]; exist {
			delete(svc.schClusters, cluster.ClusterID)
		}
		return cluster, nil
	}
}

func (svc *ConfigSvc) UpdateSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster) (*types.SchedulerCluster, error) {
	inter, err := svc.store.Update(ctx, cluster.ClusterID, cluster, store.WithResourceType(store.SchedulerCluster))
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.SchedulerCluster)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, exist := svc.schClusters[cluster.ClusterID]; exist {
		delete(svc.schClusters, cluster.ClusterID)
	}
	svc.schClusters[cluster.ClusterID] = cluster
	return cluster, nil
}

func (svc *ConfigSvc) getSchedulerCluster(ctx context.Context, clusterID string) (*types.SchedulerCluster, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.schClusters[clusterID]; exist {
		return cur, true
	}

	return nil, false
}

func (svc *ConfigSvc) GetSchedulerCluster(ctx context.Context, clusterID string) (*types.SchedulerCluster, error) {
	if cluster, exist := svc.getSchedulerCluster(ctx, clusterID); exist {
		return cluster, nil
	} else if inter, err := svc.store.Get(ctx, clusterID, store.WithResourceType(store.SchedulerCluster)); err != nil {
		return nil, err
	} else {
		cluster := inter.(*types.SchedulerCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schClusters[cluster.ClusterID]; exist {
			delete(svc.schClusters, cluster.ClusterID)
		}

		svc.schClusters[cluster.ClusterID] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) ListSchedulerClusters(ctx context.Context, opts ...store.OpOption) ([]*types.SchedulerCluster, error) {
	inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.SchedulerCluster))...)
	if err != nil {
		return nil, err
	}

	var clusters []*types.SchedulerCluster
	for _, inner := range inners {
		clusters = append(clusters, inner.(*types.SchedulerCluster))
	}

	return clusters, nil
}

func (svc *ConfigSvc) AddSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance) (*types.SchedulerInstance, error) {
	instance.InstanceID = NewUUID(SchedulerInstancePrefix)
	instance.State = InstanceInactive
	inter, err := svc.store.Add(ctx, instance.InstanceID, instance, store.WithResourceType(store.SchedulerInstance))
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.SchedulerInstance)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	svc.schInstances[instance.InstanceID] = &schedulerInstance{
		instance:      instance,
		keepAliveTime: time.Now(),
	}

	svc.identifier.Put(SchedulerInstancePrefix+instance.HostName, instance.InstanceID)
	return instance, nil
}

func (svc *ConfigSvc) DeleteSchedulerInstance(ctx context.Context, instanceID string) (*types.SchedulerInstance, error) {
	if inter, err := svc.store.Delete(ctx, instanceID, store.WithResourceType(store.SchedulerInstance)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		instance := inter.(*types.SchedulerInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.schInstances[instance.InstanceID]; exist {
			delete(svc.schInstances, instance.InstanceID)
		}
		svc.identifier.Delete(SchedulerInstancePrefix + instance.HostName)
		return instance, nil
	}
}

func (svc *ConfigSvc) UpdateSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance) (*types.SchedulerInstance, error) {
	inter, err := svc.store.Update(ctx, instance.InstanceID, instance, store.WithResourceType(store.SchedulerInstance))
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.SchedulerInstance)
	svc.mu.Lock()
	defer svc.mu.Unlock()

	var keepAliveTime time.Time
	if old, exist := svc.schInstances[instance.InstanceID]; exist {
		keepAliveTime = old.keepAliveTime
		delete(svc.schInstances, instance.InstanceID)
	} else {
		keepAliveTime = time.Now()
	}

	svc.schInstances[instance.InstanceID] = &schedulerInstance{
		instance:      instance,
		keepAliveTime: keepAliveTime,
	}

	return instance, nil
}

func (svc *ConfigSvc) getSchedulerInstance(ctx context.Context, instanceID string) (*schedulerInstance, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.schInstances[instanceID]; exist {
		return cur, true
	}

	return nil, false
}

func (svc *ConfigSvc) GetSchedulerInstance(ctx context.Context, instanceID string) (*types.SchedulerInstance, error) {
	if instance, exist := svc.getSchedulerInstance(ctx, instanceID); exist {
		return instance.instance, nil
	} else if inter, err := svc.store.Get(ctx, instanceID, store.WithResourceType(store.SchedulerInstance)); err != nil {
		return nil, err
	} else {
		instance := inter.(*types.SchedulerInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()

		var keepAliveTime time.Time
		if old, exist := svc.schInstances[instance.InstanceID]; exist {
			keepAliveTime = old.keepAliveTime
			delete(svc.schInstances, instance.InstanceID)
		} else {
			keepAliveTime = time.Now()
		}

		svc.schInstances[instance.InstanceID] = &schedulerInstance{
			instance:      instance,
			keepAliveTime: keepAliveTime,
		}
		return instance, nil
	}
}

func (svc *ConfigSvc) ListSchedulerInstances(ctx context.Context, opts ...store.OpOption) ([]*types.SchedulerInstance, error) {
	inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.SchedulerInstance))...)
	if err != nil {
		return nil, err
	}

	var instances []*types.SchedulerInstance
	for _, inner := range inners {
		instances = append(instances, inner.(*types.SchedulerInstance))
	}

	return instances, nil
}

func (svc *ConfigSvc) AddCDNCluster(ctx context.Context, cluster *types.CDNCluster) (*types.CDNCluster, error) {
	cluster.ClusterID = NewUUID(CDNClusterPrefix)
	inter, err := svc.store.Add(ctx, cluster.ClusterID, cluster, store.WithResourceType(store.CDNCluster))
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.CDNCluster)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, exist := svc.cdnClusters[cluster.ClusterID]; exist {
		delete(svc.cdnClusters, cluster.ClusterID)
	}
	svc.cdnClusters[cluster.ClusterID] = cluster
	return cluster, nil
}

func (svc *ConfigSvc) DeleteCDNCluster(ctx context.Context, clusterID string) (*types.CDNCluster, error) {
	if inter, err := svc.store.Delete(ctx, clusterID, store.WithResourceType(store.CDNCluster)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		cluster := inter.(*types.CDNCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnClusters[cluster.ClusterID]; exist {
			delete(svc.cdnClusters, cluster.ClusterID)
		}
		return cluster, nil
	}
}

func (svc *ConfigSvc) UpdateCDNCluster(ctx context.Context, cluster *types.CDNCluster) (*types.CDNCluster, error) {
	inter, err := svc.store.Update(ctx, cluster.ClusterID, cluster, store.WithResourceType(store.CDNCluster))
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.CDNCluster)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, exist := svc.cdnClusters[cluster.ClusterID]; exist {
		delete(svc.cdnClusters, cluster.ClusterID)
	}
	svc.cdnClusters[cluster.ClusterID] = cluster
	return cluster, nil
}

func (svc *ConfigSvc) getCDNCluster(ctx context.Context, clusterID string) (*types.CDNCluster, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.cdnClusters[clusterID]; exist {
		return cur, true
	}

	return nil, false
}

func (svc *ConfigSvc) GetCDNCluster(ctx context.Context, clusterID string) (*types.CDNCluster, error) {
	if cluster, exist := svc.getCDNCluster(ctx, clusterID); exist {
		return cluster, nil
	} else if inter, err := svc.store.Get(ctx, clusterID, store.WithResourceType(store.CDNCluster)); err != nil {
		return nil, err
	} else {
		cluster := inter.(*types.CDNCluster)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnClusters[cluster.ClusterID]; exist {
			delete(svc.cdnClusters, cluster.ClusterID)
		}

		svc.cdnClusters[cluster.ClusterID] = cluster
		return cluster, nil
	}
}

func (svc *ConfigSvc) ListCDNClusters(ctx context.Context, opts ...store.OpOption) ([]*types.CDNCluster, error) {
	inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.CDNCluster))...)
	if err != nil {
		return nil, err
	}

	clusters := []*types.CDNCluster{}
	for _, inner := range inners {
		clusters = append(clusters, inner.(*types.CDNCluster))
	}

	return clusters, nil
}

func (svc *ConfigSvc) AddCDNInstance(ctx context.Context, instance *types.CDNInstance) (*types.CDNInstance, error) {
	instance.InstanceID = NewUUID(CDNInstancePrefix)
	instance.State = InstanceInactive
	inter, err := svc.store.Add(ctx, instance.InstanceID, instance, store.WithResourceType(store.CDNInstance))
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.CDNInstance)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, exist := svc.cdnInstances[instance.InstanceID]; exist {
		delete(svc.cdnInstances, instance.InstanceID)
	}
	svc.cdnInstances[instance.InstanceID] = &cdnInstance{
		instance:      instance,
		keepAliveTime: time.Now(),
	}
	svc.identifier.Put(CDNInstancePrefix+instance.HostName, instance.InstanceID)
	return instance, nil
}

func (svc *ConfigSvc) DeleteCDNInstance(ctx context.Context, instanceID string) (*types.CDNInstance, error) {
	if inter, err := svc.store.Delete(ctx, instanceID, store.WithResourceType(store.CDNInstance)); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		instance := inter.(*types.CDNInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()
		if _, exist := svc.cdnInstances[instance.InstanceID]; exist {
			delete(svc.cdnInstances, instance.InstanceID)
		}
		svc.identifier.Delete(CDNInstancePrefix + instance.HostName)
		return instance, nil
	}
}

func (svc *ConfigSvc) UpdateCDNInstance(ctx context.Context, instance *types.CDNInstance) (*types.CDNInstance, error) {
	inter, err := svc.store.Update(ctx, instance.InstanceID, instance, store.WithResourceType(store.CDNInstance))
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.CDNInstance)
	svc.mu.Lock()
	defer svc.mu.Unlock()

	var keepAliveTime time.Time
	if old, exist := svc.cdnInstances[instance.InstanceID]; exist {
		keepAliveTime = old.keepAliveTime
		delete(svc.cdnInstances, instance.InstanceID)
	} else {
		keepAliveTime = time.Now()
	}

	svc.cdnInstances[instance.InstanceID] = &cdnInstance{
		instance:      instance,
		keepAliveTime: keepAliveTime,
	}
	return instance, nil
}

func (svc *ConfigSvc) getCDNInstance(ctx context.Context, instanceID string) (*cdnInstance, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.cdnInstances[instanceID]; exist {
		return cur, true
	}

	return nil, false
}

func (svc *ConfigSvc) GetCDNInstance(ctx context.Context, instanceID string) (*types.CDNInstance, error) {
	if instance, exist := svc.getCDNInstance(ctx, instanceID); exist {
		return instance.instance, nil
	} else if inter, err := svc.store.Get(ctx, instanceID, store.WithResourceType(store.CDNInstance)); err != nil {
		return nil, err
	} else {
		instance := inter.(*types.CDNInstance)
		svc.mu.Lock()
		defer svc.mu.Unlock()

		var keepAliveTime time.Time
		if old, exist := svc.cdnInstances[instance.InstanceID]; exist {
			keepAliveTime = old.keepAliveTime
			delete(svc.cdnInstances, instance.InstanceID)
		} else {
			keepAliveTime = time.Now()
		}

		svc.cdnInstances[instance.InstanceID] = &cdnInstance{
			instance:      instance,
			keepAliveTime: keepAliveTime,
		}
		return instance, nil
	}
}

func (svc *ConfigSvc) ListCDNInstances(ctx context.Context, opts ...store.OpOption) ([]*types.CDNInstance, error) {
	inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.CDNInstance))...)
	if err != nil {
		return nil, err
	}

	var instances []*types.CDNInstance
	for _, inner := range inners {
		instances = append(instances, inner.(*types.CDNInstance))
	}

	return instances, nil
}

func (svc *ConfigSvc) AddSecurityDomain(ctx context.Context, securityDomain *types.SecurityDomain) (*types.SecurityDomain, error) {
	inter, err := svc.store.Add(ctx, securityDomain.SecurityDomain, securityDomain, store.WithResourceType(store.SecurityDomain))
	if err != nil {
		return nil, err
	}

	domain := inter.(*types.SecurityDomain)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	svc.securityDomain[domain.SecurityDomain] = domain
	return domain, nil
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
	inter, err := svc.store.Update(ctx, securityDomain.SecurityDomain, securityDomain, store.WithResourceType(store.SecurityDomain))
	if err != nil {
		return nil, err
	}

	domain := inter.(*types.SecurityDomain)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if _, exist := svc.securityDomain[domain.SecurityDomain]; exist {
		delete(svc.securityDomain, domain.SecurityDomain)
	}
	svc.securityDomain[domain.SecurityDomain] = domain
	return domain, nil
}
func (svc *ConfigSvc) getSecurityDomain(ctx context.Context, securityDomain string) (*types.SecurityDomain, bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if cur, exist := svc.securityDomain[securityDomain]; exist {
		return cur, true
	}

	return nil, false
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
	inners, err := svc.store.List(ctx, append(opts, store.WithResourceType(store.SecurityDomain))...)
	if err != nil {
		return nil, err
	}

	var domains []*types.SecurityDomain
	for _, inner := range inners {
		domains = append(domains, inner.(*types.SecurityDomain))
	}

	return domains, nil
}
