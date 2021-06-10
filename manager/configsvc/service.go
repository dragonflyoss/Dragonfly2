package configsvc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/dc"
	"d7y.io/dragonfly/v2/manager/host"
	"d7y.io/dragonfly/v2/manager/hostidentifier"
	"d7y.io/dragonfly/v2/manager/lease"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
	rCache "github.com/go-redis/cache/v8"
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

type ConfigSvc struct {
	mu         sync.Mutex
	store      store.Store
	identifier hostidentifier.Identifier
	lessor     lease.Lessor

	schedulerClusters  cache.Cache
	cdnClusters        cache.Cache
	schedulerInstances cache.Cache
	cdnInstances       cache.Cache
	securityDomain     cache.Cache

	keepAliveCache      *rCache.Cache
	keepAliveTTL        time.Duration
	keepAliveTimeoutMax time.Duration
	checkKeepAliveTime  time.Duration

	keepAliveLeaseID          lease.LeaseID
	grantKeepAliveLeaseIDTime time.Duration

	stopC chan struct{}
	wg    sync.WaitGroup
}

func NewConfigSvc(store store.Store, identifier hostidentifier.Identifier, lessor lease.Lessor, client *dc.RedisClient) (*ConfigSvc, error) {
	svc := &ConfigSvc{
		store:              store,
		identifier:         identifier,
		lessor:             lessor,
		schedulerClusters:  cache.New(5*time.Minute, 5*time.Second),
		cdnClusters:        cache.New(5*time.Minute, 5*time.Second),
		schedulerInstances: cache.New(5*time.Minute, 5*time.Second),
		cdnInstances:       cache.New(5*time.Minute, 5*time.Second),
		securityDomain:     cache.New(5*time.Minute, 5*time.Second),
		stopC:              make(chan struct{}),
	}

	svc.schedulerClusters.OnEvicted(svc.schClusterOnEvicted)
	svc.cdnClusters.OnEvicted(svc.cdnClusterOnEvicted)
	svc.schedulerInstances.OnEvicted(svc.schInstanceOnEvicted)
	svc.cdnInstances.OnEvicted(svc.cdnInstanceOnEvicted)
	svc.securityDomain.OnEvicted(svc.securityDomainOnEvicted)

	/* keepAlive */
	if client.Client != nil {
		svc.keepAliveCache = rCache.New(&rCache.Options{
			Redis: client.Client,
		})
	} else {
		svc.keepAliveCache = rCache.New(&rCache.Options{
			Redis: client.ClusterClient,
		})
	}
	svc.keepAliveTTL = 5 * time.Minute
	svc.keepAliveTimeoutMax = KeepAliveTimeoutMax
	svc.checkKeepAliveTime = KeepAliveTimeoutMax / 5

	/* keepAliveLease */
	svc.keepAliveLeaseID = lease.NoLease
	svc.grantKeepAliveLeaseIDTime = 5 * time.Second

	if err := svc.rebuild(); err != nil {
		return nil, err
	}

	svc.wg.Add(2)
	go svc.grantKeepAliveLeaseLoop()
	go svc.checkKeepAliveLoop()
	return svc, nil
}

func (svc *ConfigSvc) schClusterOnEvicted(s string, i interface{}) {
	_, _ = svc.GetSchedulerCluster(context.TODO(), s, store.WithSkipLocalCache())
}

func (svc *ConfigSvc) schInstanceOnEvicted(s string, i interface{}) {
	_, _ = svc.GetSchedulerInstance(context.TODO(), s, store.WithSkipLocalCache())
}

func (svc *ConfigSvc) cdnClusterOnEvicted(s string, i interface{}) {
	_, _ = svc.GetCDNCluster(context.TODO(), s, store.WithSkipLocalCache())
}

func (svc *ConfigSvc) cdnInstanceOnEvicted(s string, i interface{}) {
	_, _ = svc.GetCDNInstance(context.TODO(), s, store.WithSkipLocalCache())
}

func (svc *ConfigSvc) securityDomainOnEvicted(s string, i interface{}) {
	_, _ = svc.GetSecurityDomain(context.TODO(), s, store.WithSkipLocalCache())
}

func (svc *ConfigSvc) grantKeepAliveLease() (lease.LeaseID, chan struct{}, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	key := "ConfigSvc/keepAliveLeaseID"
	value := iputils.HostName
	leaseID, err := svc.lessor.Grant(ctx, key, value, 10)
	if err != nil {
		logger.Infof("grant lease error: key %s, value %s, error %s", key, value, err.Error())
		return lease.NoLease, nil, err
	}

	ch, err := svc.lessor.KeepAlive(ctx, leaseID)
	if err != nil {
		svc.lessor.Revoke(ctx, leaseID)
		logger.Errorf("keepalive lease error: key %s, value %s, leaseID %s, error %s", key, value, leaseID.String(), err.Error())
		return lease.NoLease, nil, err
	}

	logger.Infof("grant lease successful: key %s, value %s, leaseID %s", key, value, leaseID.String())
	return leaseID, ch, nil
}

func (svc *ConfigSvc) setKeepAliveLeaseID(id lease.LeaseID) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	svc.keepAliveLeaseID = id
}

func (svc *ConfigSvc) getKeepAliveLeaseID() lease.LeaseID {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return svc.keepAliveLeaseID
}

func (svc *ConfigSvc) grantKeepAliveLeaseLoop() {
	defer svc.wg.Done()

	var ka chan struct{}
	id, ch, err := svc.grantKeepAliveLease()
	if err == nil {
		svc.setKeepAliveLeaseID(id)
		ka = ch
	}

	for {
		select {
		case <-svc.stopC:
			return
		case <-ka:
			svc.setKeepAliveLeaseID(lease.NoLease)
		case <-time.After(svc.grantKeepAliveLeaseIDTime):
			if svc.getKeepAliveLeaseID() == lease.NoLease {
				id, ch, err := svc.grantKeepAliveLease()
				if err == nil {
					svc.setKeepAliveLeaseID(id)
					ka = ch
				}
			}
		}
	}
}

func (svc *ConfigSvc) checkKeepAliveLoop() {
	defer svc.wg.Done()

	for {
		select {
		case <-svc.stopC:
			return
		case <-time.After(svc.checkKeepAliveTime):
			if svc.getKeepAliveLeaseID() != lease.NoLease {
				svc.updateAllInstanceState()
			}
		}
	}
}

func (svc *ConfigSvc) setKeepAlive(ctx context.Context, instanceID string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return svc.keepAliveCache.Set(&rCache.Item{
		Ctx:   ctx,
		Key:   instanceID,
		Value: time.Now(),
		TTL:   svc.keepAliveTTL,
	})
}

func (svc *ConfigSvc) getKeepAlive(ctx context.Context, instanceID string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var v time.Time
	err := svc.keepAliveCache.Get(ctx, instanceID, &v)
	if err != nil {
		return time.Now(), err
	}

	return v, err
}

func (svc *ConfigSvc) deleteKeepAlive(ctx context.Context, instanceID string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return svc.keepAliveCache.Delete(ctx, instanceID)
}

func (svc *ConfigSvc) updateAllInstanceState() {
	now := time.Now()
	for instanceID, item := range svc.schedulerInstances.Items() {
		keepAliveTime, err := svc.getKeepAlive(context.TODO(), instanceID)
		if err != nil {
			continue
		}

		if now.Before(keepAliveTime.Add(svc.keepAliveTimeoutMax)) {
			continue
		}

		instance := item.Object.(*types.SchedulerInstance)
		if state, ok := instanceNextState(instance.State, true); ok {
			inter := *instance
			inter.State = state
			_, _ = svc.UpdateSchedulerInstance(context.TODO(), &inter, store.WithKeepalive())
		}
	}

	for instanceID, item := range svc.cdnInstances.Items() {
		keepAliveTime, err := svc.getKeepAlive(context.TODO(), instanceID)
		if err != nil {
			continue
		}

		if now.Before(keepAliveTime.Add(svc.keepAliveTimeoutMax)) {
			continue
		}

		instance := item.Object.(*types.CDNInstance)
		if state, ok := instanceNextState(instance.State, true); ok {
			inter := *instance
			inter.State = state
			_, _ = svc.UpdateCDNInstance(context.TODO(), &inter, store.WithKeepalive())
		}
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
				_ = svc.schedulerClusters.Add(cluster.ClusterID, cluster, cache.DefaultExpiration)
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
				_ = svc.cdnClusters.Add(cluster.ClusterID, cluster, cache.DefaultExpiration)
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
				_ = svc.schedulerInstances.Add(instance.InstanceID, instance, cache.DefaultExpiration)
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
				_ = svc.cdnInstances.Add(instance.InstanceID, instance, cache.DefaultExpiration)
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
				_ = svc.securityDomain.Add(domain.SecurityDomain, domain, cache.DefaultExpiration)
			}
		}
	}

	return nil
}

func (svc *ConfigSvc) Close() error {
	close(svc.stopC)
	svc.wg.Wait()
	svc.stopC = nil
	return nil
}

func (svc *ConfigSvc) GetSchedulers(ctx context.Context, hostInfo *host.Info) ([]string, error) {
	var nodes []string

	for _, item := range svc.schedulerInstances.Items() {
		instance := item.Object.(*types.SchedulerInstance)
		if instance.State != InstanceActive {
			continue
		}

		if hostInfo.IsDefault() {
			nodes = append(nodes, fmt.Sprintf("%s:%d", instance.IP, instance.Port))
			continue
		}

		if instance.SecurityDomain != hostInfo.SecurityDomain {
			continue
		}

		if instance.IDC == hostInfo.IDC {
			nodes = append(nodes, fmt.Sprintf("%s:%d", instance.IP, instance.Port))
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

		if err := svc.setKeepAlive(ctx, instanceID); err != nil {
			return err
		}

		instance, err := svc.GetSchedulerInstance(ctx, instanceID)
		if err != nil {
			return dferrors.Newf(dfcodes.ManagerError, "Scheduler instance not exist, instanceID %s, error %s", instanceID, err.Error())
		}

		if state, ok := instanceNextState(instance.State, false); ok {
			inter := *instance
			inter.State = state
			_, err := svc.UpdateSchedulerInstance(ctx, &inter, store.WithKeepalive())
			return err
		}

		return nil
	} else if manager.ResourceType_Cdn == req.GetType() {
		instanceID, exist := svc.identifier.Get(CDNInstancePrefix + req.GetHostName())
		if !exist {
			return dferrors.Newf(dfcodes.ManagerError, "hostname not exist, %s", req.GetHostName())
		}

		if err := svc.setKeepAlive(ctx, instanceID); err != nil {
			return err
		}

		instance, err := svc.GetCDNInstance(ctx, instanceID)
		if err != nil {
			return dferrors.Newf(dfcodes.ManagerError, "Cdn instance not exist, instanceID %s, error %s", instanceID, err.Error())
		}

		if state, ok := instanceNextState(instance.State, false); ok {
			inter := *instance
			inter.State = state
			_, err := svc.UpdateCDNInstance(ctx, &inter, store.WithKeepalive())
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

func (svc *ConfigSvc) AddSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster, opts ...store.OpOption) (*types.SchedulerCluster, error) {
	cluster.ClusterID = NewUUID(SchedulerClusterPrefix)
	inter, err := svc.store.Add(ctx, cluster.ClusterID, cluster, append(opts, store.WithResourceType(store.SchedulerCluster))...)
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.SchedulerCluster)
	_ = svc.schedulerClusters.Add(cluster.ClusterID, cluster, cache.DefaultExpiration)
	return cluster, nil
}

func (svc *ConfigSvc) DeleteSchedulerCluster(ctx context.Context, clusterID string, opts ...store.OpOption) (*types.SchedulerCluster, error) {
	if inter, err := svc.store.Delete(ctx, clusterID, append(opts, store.WithResourceType(store.SchedulerCluster))...); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		cluster := inter.(*types.SchedulerCluster)
		svc.schedulerClusters.Delete(cluster.ClusterID)
		return cluster, nil
	}
}

func (svc *ConfigSvc) UpdateSchedulerCluster(ctx context.Context, cluster *types.SchedulerCluster, opts ...store.OpOption) (*types.SchedulerCluster, error) {
	inter, err := svc.store.Update(ctx, cluster.ClusterID, cluster, append(opts, store.WithResourceType(store.SchedulerCluster))...)
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.SchedulerCluster)
	svc.schedulerClusters.SetDefault(cluster.ClusterID, cluster)
	return cluster, nil
}

func (svc *ConfigSvc) GetSchedulerCluster(ctx context.Context, clusterID string, opts ...store.OpOption) (*types.SchedulerCluster, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	if !op.SkipLocalCache {
		if cluster, _, exist := svc.schedulerClusters.GetWithExpiration(clusterID); exist {
			return cluster.(*types.SchedulerCluster), nil
		}
	}

	inter, err := svc.store.Get(ctx, clusterID, append(opts, store.WithResourceType(store.SchedulerCluster))...)
	if err != nil {
		return nil, err
	}

	cluster := inter.(*types.SchedulerCluster)
	svc.schedulerClusters.SetDefault(cluster.ClusterID, cluster)
	return cluster, nil
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

func (svc *ConfigSvc) AddSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance, opts ...store.OpOption) (*types.SchedulerInstance, error) {
	instance.InstanceID = NewUUID(SchedulerInstancePrefix)
	instance.State = InstanceInactive
	inter, err := svc.store.Add(ctx, instance.InstanceID, instance, append(opts, store.WithResourceType(store.SchedulerInstance))...)
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.SchedulerInstance)
	_ = svc.schedulerInstances.Add(instance.InstanceID, instance, cache.DefaultExpiration)
	svc.identifier.Put(SchedulerInstancePrefix+instance.HostName, instance.InstanceID)
	_ = svc.setKeepAlive(ctx, instance.InstanceID)
	return instance, nil
}

func (svc *ConfigSvc) DeleteSchedulerInstance(ctx context.Context, instanceID string, opts ...store.OpOption) (*types.SchedulerInstance, error) {
	if inter, err := svc.store.Delete(ctx, instanceID, append(opts, store.WithResourceType(store.SchedulerInstance))...); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		instance := inter.(*types.SchedulerInstance)
		svc.schedulerInstances.Delete(instance.InstanceID)
		svc.identifier.Delete(SchedulerInstancePrefix + instance.HostName)
		_ = svc.deleteKeepAlive(ctx, instance.InstanceID)
		return instance, nil
	}
}

func (svc *ConfigSvc) UpdateSchedulerInstance(ctx context.Context, instance *types.SchedulerInstance, opts ...store.OpOption) (*types.SchedulerInstance, error) {
	inter, err := svc.store.Update(ctx, instance.InstanceID, instance, append(opts, store.WithResourceType(store.SchedulerInstance))...)
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.SchedulerInstance)
	svc.schedulerInstances.SetDefault(instance.InstanceID, instance)
	return instance, nil
}

func (svc *ConfigSvc) GetSchedulerInstance(ctx context.Context, instanceID string, opts ...store.OpOption) (*types.SchedulerInstance, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	if !op.SkipLocalCache {
		if instance, _, exist := svc.schedulerInstances.GetWithExpiration(instanceID); exist {
			return instance.(*types.SchedulerInstance), nil
		}
	}

	inter, err := svc.store.Get(ctx, instanceID, append(opts, store.WithResourceType(store.SchedulerInstance))...)
	if err != nil {
		return nil, err
	}

	instance := inter.(*types.SchedulerInstance)
	svc.schedulerInstances.SetDefault(instance.InstanceID, instance)
	return instance, nil
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

func (svc *ConfigSvc) AddCDNCluster(ctx context.Context, cluster *types.CDNCluster, opts ...store.OpOption) (*types.CDNCluster, error) {
	cluster.ClusterID = NewUUID(CDNClusterPrefix)
	inter, err := svc.store.Add(ctx, cluster.ClusterID, cluster, append(opts, store.WithResourceType(store.CDNCluster))...)
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.CDNCluster)
	_ = svc.cdnClusters.Add(cluster.ClusterID, cluster, cache.DefaultExpiration)
	return cluster, nil
}

func (svc *ConfigSvc) DeleteCDNCluster(ctx context.Context, clusterID string, opts ...store.OpOption) (*types.CDNCluster, error) {
	if inter, err := svc.store.Delete(ctx, clusterID, append(opts, store.WithResourceType(store.CDNCluster))...); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		cluster := inter.(*types.CDNCluster)
		svc.cdnClusters.Delete(cluster.ClusterID)
		return cluster, nil
	}
}

func (svc *ConfigSvc) UpdateCDNCluster(ctx context.Context, cluster *types.CDNCluster, opts ...store.OpOption) (*types.CDNCluster, error) {
	inter, err := svc.store.Update(ctx, cluster.ClusterID, cluster, append(opts, store.WithResourceType(store.CDNCluster))...)
	if err != nil {
		return nil, err
	}

	cluster = inter.(*types.CDNCluster)
	svc.cdnClusters.SetDefault(cluster.ClusterID, cluster)
	return cluster, nil
}

func (svc *ConfigSvc) GetCDNCluster(ctx context.Context, clusterID string, opts ...store.OpOption) (*types.CDNCluster, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	if !op.SkipLocalCache {
		if cluster, _, exist := svc.cdnClusters.GetWithExpiration(clusterID); exist {
			return cluster.(*types.CDNCluster), nil
		}
	}

	inter, err := svc.store.Get(ctx, clusterID, append(opts, store.WithResourceType(store.CDNCluster))...)
	if err != nil {
		return nil, err
	}

	cluster := inter.(*types.CDNCluster)
	svc.cdnClusters.SetDefault(cluster.ClusterID, cluster)
	return cluster, nil
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

func (svc *ConfigSvc) AddCDNInstance(ctx context.Context, instance *types.CDNInstance, opts ...store.OpOption) (*types.CDNInstance, error) {
	instance.InstanceID = NewUUID(CDNInstancePrefix)
	instance.State = InstanceInactive
	inter, err := svc.store.Add(ctx, instance.InstanceID, instance, append(opts, store.WithResourceType(store.CDNInstance))...)
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.CDNInstance)
	_ = svc.cdnInstances.Add(instance.InstanceID, instance, cache.DefaultExpiration)
	svc.identifier.Put(CDNInstancePrefix+instance.HostName, instance.InstanceID)
	_ = svc.setKeepAlive(ctx, instance.InstanceID)
	return instance, nil
}

func (svc *ConfigSvc) DeleteCDNInstance(ctx context.Context, instanceID string, opts ...store.OpOption) (*types.CDNInstance, error) {
	if inter, err := svc.store.Delete(ctx, instanceID, append(opts, store.WithResourceType(store.CDNInstance))...); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		instance := inter.(*types.CDNInstance)
		svc.cdnInstances.Delete(instance.InstanceID)
		svc.identifier.Delete(CDNInstancePrefix + instance.HostName)
		_ = svc.deleteKeepAlive(ctx, instance.InstanceID)
		return instance, nil
	}
}

func (svc *ConfigSvc) UpdateCDNInstance(ctx context.Context, instance *types.CDNInstance, opts ...store.OpOption) (*types.CDNInstance, error) {
	inter, err := svc.store.Update(ctx, instance.InstanceID, instance, append(opts, store.WithResourceType(store.CDNInstance))...)
	if err != nil {
		return nil, err
	}

	instance = inter.(*types.CDNInstance)
	svc.cdnInstances.SetDefault(instance.InstanceID, instance)
	return instance, nil
}

func (svc *ConfigSvc) GetCDNInstance(ctx context.Context, instanceID string, opts ...store.OpOption) (*types.CDNInstance, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	if !op.SkipLocalCache {
		if instance, _, exist := svc.cdnInstances.GetWithExpiration(instanceID); exist {
			return instance.(*types.CDNInstance), nil
		}
	}

	inter, err := svc.store.Get(ctx, instanceID, append(opts, store.WithResourceType(store.CDNInstance))...)
	if err != nil {
		return nil, err
	}

	instance := inter.(*types.CDNInstance)
	svc.cdnInstances.SetDefault(instance.InstanceID, instance)
	return instance, nil
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

func (svc *ConfigSvc) AddSecurityDomain(ctx context.Context, securityDomain *types.SecurityDomain, opts ...store.OpOption) (*types.SecurityDomain, error) {
	inter, err := svc.store.Add(ctx, securityDomain.SecurityDomain, securityDomain, append(opts, store.WithResourceType(store.SecurityDomain))...)
	if err != nil {
		return nil, err
	}

	domain := inter.(*types.SecurityDomain)
	_ = svc.securityDomain.Add(domain.SecurityDomain, domain, cache.DefaultExpiration)
	return domain, nil
}

func (svc *ConfigSvc) DeleteSecurityDomain(ctx context.Context, securityDomain string, opts ...store.OpOption) (*types.SecurityDomain, error) {
	if inter, err := svc.store.Delete(ctx, securityDomain, append(opts, store.WithResourceType(store.SecurityDomain))...); err != nil {
		return nil, err
	} else if inter == nil {
		return nil, nil
	} else {
		domain := inter.(*types.SecurityDomain)
		svc.securityDomain.Delete(domain.SecurityDomain)
		return domain, nil
	}
}

func (svc *ConfigSvc) UpdateSecurityDomain(ctx context.Context, securityDomain *types.SecurityDomain, opts ...store.OpOption) (*types.SecurityDomain, error) {
	inter, err := svc.store.Update(ctx, securityDomain.SecurityDomain, securityDomain, append(opts, store.WithResourceType(store.SecurityDomain))...)
	if err != nil {
		return nil, err
	}

	domain := inter.(*types.SecurityDomain)
	svc.securityDomain.SetDefault(domain.SecurityDomain, domain)
	return domain, nil
}

func (svc *ConfigSvc) GetSecurityDomain(ctx context.Context, securityDomain string, opts ...store.OpOption) (*types.SecurityDomain, error) {
	op := store.Op{}
	op.ApplyOpts(opts)

	if !op.SkipLocalCache {
		if domain, _, exist := svc.securityDomain.GetWithExpiration(securityDomain); exist {
			return domain.(*types.SecurityDomain), nil
		}
	}

	inter, err := svc.store.Get(ctx, securityDomain, append(opts, store.WithResourceType(store.SecurityDomain))...)
	if err != nil {
		return nil, err
	}

	domain := inter.(*types.SecurityDomain)
	svc.securityDomain.SetDefault(domain.SecurityDomain, domain)
	return domain, nil
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
