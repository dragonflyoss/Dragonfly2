package networktopology

import (
	"container/list"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sort"
	"strings"
	"sync"
	"time"
)

// The priority is incremented by 10 because involving scores and the argument in sort.slice must be an integer.
const (
	// SecurityDomainPriority is the priority of host securityDomain.
	SecurityDomainPriority = 10

	// IDCPriority is the priority of host IDC.
	IDCPriority = 20

	// IPPriority is the priority of host IP.
	IPPriority = 30

	// LocationPriority is the priority of host location.
	LocationPriority = 40

	// AOIPriority is the priority of host age of information.
	AOIPriority = 50
)

type NetworkTopology interface {

	// GetHost returns host from hostId.
	GetHost(string) (*resource.Host, bool)

	// LoadParents returns host for a key.
	LoadParents(string) (*sync.Map, bool)

	// StoreParents sets host.
	StoreParents(key string, parents *sync.Map)

	// DeleteParents deletes host for a key.
	DeleteParents(string)

	// LoadEdge returns edge between tow hosts.
	LoadEdge(src, dest string) (*Probes, bool)

	// StoreEdge sets edge between tow hosts.
	StoreEdge(src, dest string, probes *Probes)

	// DeleteEdge deletes edge between tow hosts.
	DeleteEdge(src, dest string)

	// LoadAverageRTT returns the average round-trip time of the specified edge.
	LoadAverageRTT(src, dest string) (time.Duration, bool)

	// StoreProbe store a probe in edge.
	StoreProbe(
		src, dest string, Host *resource.Host,
		RTT *durationpb.Duration, UpdatedAt *timestamppb.Timestamp)

	// ToMatrix transform map to matrix for finding the shortest paths.
	ToMatrix() ([][]time.Duration, int, map[string]int, error)

	// FindShortestPaths finds the shortest paths from src to each dest.
	FindShortestPaths(src string, dests []string) []time.Duration

	// FindProbes finds the probe targets for host.
	FindProbes(host *resource.Host) []*resource.Host

	// Serve starts networktopology server.
	Serve() error

	// Stop networktopology server.
	Stop() error
}

type networkTopology struct {
	*sync.Map
	resource resource.Resource
	done     chan struct{}
}

// New network topology interface.
func New(resource resource.Resource) (NetworkTopology, error) {

	n := &networkTopology{
		Map:      &sync.Map{},
		resource: resource,
	}
	return n, nil
}

func (n *networkTopology) GetHost(hostId string) (*resource.Host, bool) {
	host, ok := n.resource.HostManager().Load(hostId)
	if ok {
		return host, ok
	}
	return nil, ok
}

func (n *networkTopology) LoadParents(key string) (*sync.Map, bool) {
	rawNetwork, ok := n.Map.Load(key)
	if !ok {
		return nil, false
	}
	return rawNetwork.(*sync.Map), ok
}

func (n *networkTopology) StoreParents(key string, parents *sync.Map) {
	n.Map.Store(key, parents)
}

func (n *networkTopology) DeleteParents(key string) {
	n.Map.Delete(key)
}

func (n *networkTopology) LoadEdge(src, dest string) (*Probes, bool) {

	parents, ok := n.Map.Load(src)
	if ok {
		probes, ok1 := parents.(*sync.Map).Load(dest)
		if ok1 {
			return probes.(*Probes), ok && ok1
		}
	}
	return nil, false
}

func (n *networkTopology) LoadAverageRTT(src, dest string) (time.Duration, bool) {

	parents, ok := n.Map.Load(src)
	if ok {
		probes, ok1 := parents.(*sync.Map).Load(dest)
		if ok1 {
			return probes.(*Probes).AverageRTT, ok && ok1
		}
	}
	return time.Duration(0), false
}

func (n *networkTopology) StoreEdge(src, dest string, probes *Probes) {

	parents, ok := n.Map.Load(src)
	if ok {
		parents.(*sync.Map).Store(dest, probes)
	}
}

func (n *networkTopology) DeleteEdge(src, dest string) {

	rawNetwork, ok := n.Map.Load(src)
	if ok {
		rawNetwork.(*sync.Map).Delete(dest)
	}
}

func (n *networkTopology) StoreProbe(
	src, dest string, Host *resource.Host,
	RTT *durationpb.Duration, UpdatedAt *timestamppb.Timestamp) {
	rawNetwork, ok := n.Map.Load(src)
	if ok {
		rawProbes, ok1 := rawNetwork.(*sync.Map).Load(dest)
		if ok1 {
			rawProbe := newProbe(Host, RTT, UpdatedAt)
			rawProbes.(*Probes).StoreProbe(rawProbe)
		}
	}
}

func (n *networkTopology) ToMatrix() ([][]time.Duration, int, map[string]int, error) {
	var matrix [][]time.Duration
	// Records the relationship between the matrix index and the Host ID.
	Key2Index := make(map[string]int)

	// Load hosts in the network topology into Key2Index
	//n.Map.Range(func(src, value interface{}) bool {
	//	key := src.(string)
	//	Key2Index[key] = 0
	//	value.(*sync.Map).Range(func(dest, probes interface{}) bool {
	//		Key2Index[dest.(string)] = 0
	//		return true
	//	})
	//	return true
	//})
	//
	//// keys records the host id.
	//keys := make([]string, 0)
	//for k := range Key2Index {
	//	keys = append(keys, k)
	//}
	//i := 0
	//for _, v := range keys {
	//	Key2Index[v] = i
	//	i++
	//}

	keys := make([]string, 0)
	n.Map.Range(func(src, value interface{}) bool {
		keys = append(keys, src.(string))
		return true
	})
	i := 0
	for _, v := range keys {
		Key2Index[v] = i
		i++
	}

	hostCount := len(Key2Index)
	// Initializes the matrix
	for i := 0; i < hostCount; i++ {
		tmp := make([]time.Duration, hostCount)
		matrix = append(matrix, tmp)
	}
	// Initializes the value of the matrix
	for i := 0; i < hostCount; i++ {
		for j := 0; j < hostCount; j++ {
			// TODO: 1 * time.Second is default ping timeout.
			matrix[i][j] = 1 * time.Second
		}
	}
	n.Map.Range(func(src, value interface{}) bool {
		value.(*sync.Map).Range(func(dest, probes any) bool {
			matrix[Key2Index[src.(string)]][Key2Index[dest.(string)]] = probes.(*Probes).AverageRTT
			return true
		})
		return true
	})
	return matrix, hostCount, Key2Index, nil
}

func (n *networkTopology) FindShortestPaths(src string, dests []string) []time.Duration {
	matrix, hostCount, key2Index, err := n.ToMatrix()
	if err != nil {
		return nil
	}

	// The distance between the src and others in network topology.
	dis := make([]time.Duration, hostCount)
	// Mark whether each host is accessed.
	vis := make([]bool, hostCount)
	for i := range dis {
		// TODO: 1 * time.Second is default ping timeout.
		dis[i] = 1 * time.Second
	}
	queue := list.New()
	dis[key2Index[src]] = 0
	vis[key2Index[src]] = true

	queue.PushBack(key2Index[src])
	for queue.Len() != 0 {
		// pop
		u := queue.Front()
		queue.Remove(u)

		vis[u.Value.(int)] = false
		for v := 0; v < hostCount; v++ {
			if dis[v] > dis[u.Value.(int)]+matrix[u.Value.(int)][v] {
				dis[v] = dis[u.Value.(int)] + matrix[u.Value.(int)][v]
				if !vis[v] {
					queue.PushBack(v)
					vis[v] = true
				}
			}
		}
	}

	toDestsRTT := make([]time.Duration, 0)
	for _, dest := range dests {
		toDestsRTT = append(toDestsRTT, dis[key2Index[dest]])
	}
	return toDestsRTT
}

func (n *networkTopology) FindProbes(host *resource.Host) []*resource.Host {
	hosts := make([]*resource.Host, 0)
	n.Map.Range(func(src, value interface{}) bool {
		rawHost, ok := n.resource.HostManager().Load(src.(string))
		if ok && host.ID != src.(string) {
			hosts = append(hosts, rawHost)
		}
		return true
	})

	if len(hosts) <= config.DefaultProbeSyncCount {
		return hosts
	}

	// The priority of hosts.
	var (
		hostPriority map[*resource.Host]int
		rawLocation  = strings.Split(host.Network.Location, "|")
		flag         = false
	)

	for _, h := range hosts {
		if h.Network.SecurityDomain == host.Network.SecurityDomain {
			hostPriority[h] = SecurityDomainPriority
			continue
		}
		if h.Network.IDC == host.Network.IDC {
			hostPriority[h] = IDCPriority
			continue
		}
		if h.IP == host.IP {
			hostPriority[h] = IPPriority
			continue
		}
		flag = false
		hostLocation := strings.Split(host.Network.Location, "|")
		for index := range rawLocation {
			if rawLocation[index] != hostLocation[index] {
				hostPriority[h] = LocationPriority + 10 - index/len(rawLocation)
				flag = true
				break
			}
		}
		if !flag {
			edge, ok := n.LoadEdge(host.ID, h.ID)
			if ok {
				//TODO: Here we need to design a timestamp measurement point.
				hostPriority[h] = AOIPriority + 1 -
					int(time.Now().Sub(edge.GetUpdatedAt())/time.Now().Sub(time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)))
			}
		}
	}
	sort.Slice(hosts, func(i, j int) bool {
		return hostPriority[hosts[i]] < hostPriority[hosts[j]]
	})
	return hosts[0:config.DefaultProbeSyncCount]
}

func (n *networkTopology) Serve() error {

	//var (
	//	grpcCredentials credentials.TransportCredentials
	//	certifyClient  *certify.Certify
	//)
	//
	//if certifyClient == nil {
	//	grpcCredentials = insecure.NewCredentials()
	//} else {
	//	grpcCredentials, err = loadGlobalGPRCTLSCredentials(certifyClient, opt.Security)
	//	if err != nil {
	//		return err
	//	}
	//}
	//// Initialize scheduler client.
	//schedulerClient, err = schedulerclient.GetV1ByAddr(context.Background(), opt.Scheduler.Manager.NetAddrs, grpc.WithTransportCredentials(grpcCredentials))
	//if err != nil {
	//	return err
	//}
	//
	//if err := n.schedulerClient; err != nil {
	//	return err
	//}
	//tick := time.NewTicker(config.DefaultNetworkTopologySyncInterval)
	//for {
	//	select {
	//	case <-tick.C:
	//		for name, gc := range  {
	//
	//		}
	//	case <-n.done:
	//		return nil
	//	}
	//}
	return nil
}

func (n *networkTopology) Stop() error {
	close(n.done)
	return nil
}
