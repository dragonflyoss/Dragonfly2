package networktopology

import (
	"container/list"
	"context"
	v2 "d7y.io/api/pkg/apis/common/v2"
	managerv2 "d7y.io/api/pkg/apis/manager/v2"
	schedulerv2 "d7y.io/api/pkg/apis/scheduler/v2"
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

	// StoreSyncHost stores the hosts that sent the synchronization probe instead of
	// the hosts that synchronized the network topology.
	StoreSyncHost(string)

	// DeleteSyncHost deletes the hosts that sent the synchronization probe, it comes
	// from LeaveHost.
	DeleteSyncHost(string)

	// LoadParents returns host for a key.
	LoadParents(string) (*sync.Map, bool)

	// StoreParents stores host.
	StoreParents(key string, parents *sync.Map)

	// DeleteParents deletes host for a key.
	DeleteParents(string)

	// LoadEdge returns edge between tow hosts.
	LoadEdge(src, dest string) (*Probes, bool)

	// StoreEdge stores edge between two hosts.
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

	// SyncNetworkTopology synchronizes the network topology to other schedulers.
	SyncNetworkTopology() error

	// Serve starts networktopology server.
	Serve() error

	// Stop networktopology server.
	Stop() error
}

type empty struct{}

type networkTopology struct {
	*sync.Map

	// Scheduler config.
	config *config.Config

	// Resource interface
	resource resource.Resource

	// Manager client interface
	managerClient managerclient.V2

	// TransportCredentials stores the Authenticator required to set up a client connection.
	transportCredentials credentials.TransportCredentials

	// LocalHosts stores the host from which the probe is sent
	localHosts map[string]empty

	// deleteHosts stores the deleted host in a synchronized time interval.
	deleteHosts []string

	done chan struct{}
}

// Option is a functional option for configuring the networkTopology.
type Option func(n *networkTopology)

// WithTransportCredentials returns a DialOption which configures a connection
// level security credentials (e.g., TLS/SSL).
func WithTransportCredentials(creds credentials.TransportCredentials) Option {
	return func(n *networkTopology) {
		n.transportCredentials = creds
	}
}

// New network topology interface.
func New(cfg *config.Config, resource resource.Resource, managerClient managerclient.V2, options ...Option) (NetworkTopology, error) {

	n := &networkTopology{config: cfg}

	for _, opt := range options {
		opt(n)
	}
	n.Map = &sync.Map{}
	n.resource = resource
	n.managerClient = managerClient
	n.localHosts = map[string]empty{}
	n.deleteHosts = make([]string, 0)
	return n, nil
}

func (n *networkTopology) GetHost(hostId string) (*resource.Host, bool) {
	host, ok := n.resource.HostManager().Load(hostId)
	if ok {
		return host, ok
	}
	return nil, ok
}

func (n *networkTopology) StoreSyncHost(hostId string) {

	n.localHosts[hostId] = empty{}
}

func (n *networkTopology) DeleteSyncHost(hostId string) {

	n.deleteHosts = append(n.deleteHosts, hostId)
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
			rawProbe := NewProbe(Host, RTT, UpdatedAt)
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
				updatedAt, ok1 := edge.GetUpdatedAt()
				if ok1 {
					hostPriority[h] = AOIPriority + 1 -
						int(time.Now().Sub(updatedAt)/time.Now().Sub(InitTime))
				}
			}
		}
	}
	sort.Slice(hosts, func(i, j int) bool {
		return hostPriority[hosts[i]] < hostPriority[hosts[j]]
	})
	return hosts[0:config.DefaultProbeSyncCount]
}

func (n *networkTopology) SyncNetworkTopology() error {

	listSchedulersResponse, err := n.managerClient.ListSchedulers(context.Background(), &managerv2.ListSchedulersRequest{
		SourceType: managerv2.SourceType_SCHEDULER_SOURCE,
		HostName:   n.config.Server.Host,
		Ip:         n.config.Server.AdvertiseIP.String(),
		HostInfo:   nil,
		Version:    version.GitVersion,
		Commit:     version.GitCommit,
	})
	if err != nil {
		return nil
	}

	if n.config.NetworkTopology.Enable {
		// Generate grpc synchronization network topology request of updating hosts.
		updateProbesOfHosts := make([]*schedulerv2.ProbesOfHost, 0)
		for srcID := range n.localHosts {
			parents, ok := n.Load(srcID)
			if ok {
				parents.(*sync.Map).Range(func(dest, probes interface{}) bool {
					rawHost, ok1 := n.GetHost(dest.(string))
					rawProbes := make([]*schedulerv2.Probe, 0)
					for e := probes.(Probes).Probes.Front(); e != nil; e = e.Next() {
						probe := e.Value.(Probe)

						p := &schedulerv2.Probe{
							Host: &v2.Host{
								Id:              probe.Host.ID,
								Type:            uint32(probe.Host.Type),
								Hostname:        probe.Host.Hostname,
								Ip:              probe.Host.IP,
								Port:            probe.Host.Port,
								DownloadPort:    probe.Host.DownloadPort,
								Os:              probe.Host.OS,
								Platform:        probe.Host.Platform,
								PlatformFamily:  probe.Host.PlatformFamily,
								PlatformVersion: probe.Host.PlatformVersion,
								KernelVersion:   probe.Host.KernelVersion,
								Cpu: &v2.CPU{
									LogicalCount:   probe.Host.CPU.LogicalCount,
									PhysicalCount:  probe.Host.CPU.PhysicalCount,
									Percent:        probe.Host.CPU.Percent,
									ProcessPercent: probe.Host.CPU.ProcessPercent,
									Times: &v2.CPUTimes{
										User:      probe.Host.CPU.Times.User,
										System:    probe.Host.CPU.Times.System,
										Idle:      probe.Host.CPU.Times.Idle,
										Nice:      probe.Host.CPU.Times.Nice,
										Iowait:    probe.Host.CPU.Times.Iowait,
										Irq:       probe.Host.CPU.Times.Irq,
										Softirq:   probe.Host.CPU.Times.Softirq,
										Steal:     probe.Host.CPU.Times.Steal,
										Guest:     probe.Host.CPU.Times.Guest,
										GuestNice: probe.Host.CPU.Times.GuestNice,
									},
								},
								Memory: &v2.Memory{
									Total:              probe.Host.Memory.Total,
									Available:          probe.Host.Memory.Available,
									Used:               probe.Host.Memory.Used,
									UsedPercent:        probe.Host.Memory.UsedPercent,
									ProcessUsedPercent: probe.Host.Memory.ProcessUsedPercent,
									Free:               probe.Host.Memory.Free,
								},
								Network: &v2.Network{
									TcpConnectionCount:       probe.Host.Network.TCPConnectionCount,
									UploadTcpConnectionCount: probe.Host.Network.UploadTCPConnectionCount,
									SecurityDomain:           probe.Host.Network.SecurityDomain,
									Location:                 probe.Host.Network.Location,
									Idc:                      probe.Host.Network.IDC,
								},
								Disk: &v2.Disk{
									Total:             probe.Host.Disk.Total,
									Free:              probe.Host.Disk.Free,
									Used:              probe.Host.Disk.Used,
									UsedPercent:       probe.Host.Disk.UsedPercent,
									InodesTotal:       probe.Host.Disk.InodesTotal,
									InodesUsed:        probe.Host.Disk.InodesUsed,
									InodesFree:        probe.Host.Disk.InodesFree,
									InodesUsedPercent: probe.Host.Disk.InodesUsedPercent,
								},
								Build: &v2.Build{
									GitVersion: probe.Host.Build.GitVersion,
									GitCommit:  probe.Host.Build.GitCommit,
									GoVersion:  probe.Host.Build.GoVersion,
									Platform:   probe.Host.Build.Platform,
								},
							},
							Rtt:       durationpb.New(probe.RTT),
							UpdatedAt: timestamppb.New(probe.UpdatedAt),
						}
						rawProbes = append(rawProbes, p)
					}

					if ok1 {
						probesOfHost := &schedulerv2.ProbesOfHost{
							Host: &v2.Host{
								Id:              rawHost.ID,
								Type:            uint32(rawHost.Type),
								Hostname:        rawHost.Hostname,
								Ip:              rawHost.IP,
								Port:            rawHost.Port,
								DownloadPort:    rawHost.DownloadPort,
								Os:              rawHost.OS,
								Platform:        rawHost.Platform,
								PlatformFamily:  rawHost.PlatformFamily,
								PlatformVersion: rawHost.PlatformVersion,
								KernelVersion:   rawHost.KernelVersion,
								Cpu: &v2.CPU{
									LogicalCount:   rawHost.CPU.LogicalCount,
									PhysicalCount:  rawHost.CPU.PhysicalCount,
									Percent:        rawHost.CPU.Percent,
									ProcessPercent: rawHost.CPU.ProcessPercent,
									Times: &v2.CPUTimes{
										User:      rawHost.CPU.Times.User,
										System:    rawHost.CPU.Times.System,
										Idle:      rawHost.CPU.Times.Idle,
										Nice:      rawHost.CPU.Times.Nice,
										Iowait:    rawHost.CPU.Times.Iowait,
										Irq:       rawHost.CPU.Times.Irq,
										Softirq:   rawHost.CPU.Times.Softirq,
										Steal:     rawHost.CPU.Times.Steal,
										Guest:     rawHost.CPU.Times.Guest,
										GuestNice: rawHost.CPU.Times.GuestNice,
									},
								},
								Memory: &v2.Memory{
									Total:              rawHost.Memory.Total,
									Available:          rawHost.Memory.Available,
									Used:               rawHost.Memory.Used,
									UsedPercent:        rawHost.Memory.UsedPercent,
									ProcessUsedPercent: rawHost.Memory.ProcessUsedPercent,
									Free:               rawHost.Memory.Free,
								},
								Network: &v2.Network{
									TcpConnectionCount:       rawHost.Network.TCPConnectionCount,
									UploadTcpConnectionCount: rawHost.Network.UploadTCPConnectionCount,
									SecurityDomain:           rawHost.Network.SecurityDomain,
									Location:                 rawHost.Network.Location,
									Idc:                      rawHost.Network.IDC,
								},
								Disk: &v2.Disk{
									Total:             rawHost.Disk.Total,
									Free:              rawHost.Disk.Free,
									Used:              rawHost.Disk.Used,
									UsedPercent:       rawHost.Disk.UsedPercent,
									InodesTotal:       rawHost.Disk.InodesTotal,
									InodesUsed:        rawHost.Disk.InodesUsed,
									InodesFree:        rawHost.Disk.InodesFree,
									InodesUsedPercent: rawHost.Disk.InodesUsedPercent,
								},
								Build: &v2.Build{
									GitVersion: rawHost.Build.GitVersion,
									GitCommit:  rawHost.Build.GitCommit,
									GoVersion:  rawHost.Build.GoVersion,
									Platform:   rawHost.Build.Platform,
								},
							},
							Probes: rawProbes,
						}
						updateProbesOfHosts = append(updateProbesOfHosts, probesOfHost)
					}
					return true
				})
			}
		}

		// Generate grpc synchronization network topology request of deleting hosts.
		deleteProbesOfHosts := make([]*schedulerv2.ProbesOfHost, 0)
		if len(n.deleteHosts) != 0 {
			for _, deleteHost := range n.deleteHosts {
				rawHost, ok := n.GetHost(deleteHost)
				if ok {
					probesOfHost := &schedulerv2.ProbesOfHost{
						Host: &v2.Host{
							Id:              rawHost.ID,
							Type:            uint32(rawHost.Type),
							Hostname:        rawHost.Hostname,
							Ip:              rawHost.IP,
							Port:            rawHost.Port,
							DownloadPort:    rawHost.DownloadPort,
							Os:              rawHost.OS,
							Platform:        rawHost.Platform,
							PlatformFamily:  rawHost.PlatformFamily,
							PlatformVersion: rawHost.PlatformVersion,
							KernelVersion:   rawHost.KernelVersion,
							Cpu: &v2.CPU{
								LogicalCount:   rawHost.CPU.LogicalCount,
								PhysicalCount:  rawHost.CPU.PhysicalCount,
								Percent:        rawHost.CPU.Percent,
								ProcessPercent: rawHost.CPU.ProcessPercent,
								Times: &v2.CPUTimes{
									User:      rawHost.CPU.Times.User,
									System:    rawHost.CPU.Times.System,
									Idle:      rawHost.CPU.Times.Idle,
									Nice:      rawHost.CPU.Times.Nice,
									Iowait:    rawHost.CPU.Times.Iowait,
									Irq:       rawHost.CPU.Times.Irq,
									Softirq:   rawHost.CPU.Times.Softirq,
									Steal:     rawHost.CPU.Times.Steal,
									Guest:     rawHost.CPU.Times.Guest,
									GuestNice: rawHost.CPU.Times.GuestNice,
								},
							},
							Memory: &v2.Memory{
								Total:              rawHost.Memory.Total,
								Available:          rawHost.Memory.Available,
								Used:               rawHost.Memory.Used,
								UsedPercent:        rawHost.Memory.UsedPercent,
								ProcessUsedPercent: rawHost.Memory.ProcessUsedPercent,
								Free:               rawHost.Memory.Free,
							},
							Network: &v2.Network{
								TcpConnectionCount:       rawHost.Network.TCPConnectionCount,
								UploadTcpConnectionCount: rawHost.Network.UploadTCPConnectionCount,
								SecurityDomain:           rawHost.Network.SecurityDomain,
								Location:                 rawHost.Network.Location,
								Idc:                      rawHost.Network.IDC,
							},
							Disk: &v2.Disk{
								Total:             rawHost.Disk.Total,
								Free:              rawHost.Disk.Free,
								Used:              rawHost.Disk.Used,
								UsedPercent:       rawHost.Disk.UsedPercent,
								InodesTotal:       rawHost.Disk.InodesTotal,
								InodesUsed:        rawHost.Disk.InodesUsed,
								InodesFree:        rawHost.Disk.InodesFree,
								InodesUsedPercent: rawHost.Disk.InodesUsedPercent,
							},
							Build: &v2.Build{
								GitVersion: rawHost.Build.GitVersion,
								GitCommit:  rawHost.Build.GitCommit,
								GoVersion:  rawHost.Build.GoVersion,
								Platform:   rawHost.Build.Platform,
							},
						},
						Probes: nil,
					}
					deleteProbesOfHosts = append(deleteProbesOfHosts, probesOfHost)
				}
			}
		}

		for _, scheduler := range listSchedulersResponse.Schedulers {
			schedulerClient, err := schedulerclient.GetV2ByAddr(context.Background(), scheduler.Ip, grpc.WithTransportCredentials(n.transportCredentials))
			if err != nil {
				return err
			}

			// Synchronize network topology by updating hosts.
			updateHostsRequest := &schedulerv2.UpdateHostsRequest{ProbesOfHosts: updateProbesOfHosts}
			updateProbesOfHostsRequest := &schedulerv2.SyncNetworkTopologyRequest_UpdateProbesOfHostsRequest{
				UpdateProbesOfHostsRequest: updateHostsRequest,
			}
			schedulerSyncNetworkTopologyClient, err := schedulerClient.SyncNetworkTopology(context.Background(),
				&schedulerv2.SyncNetworkTopologyRequest{Request: updateProbesOfHostsRequest})
			if err != nil {
				return err
			}

			// Synchronize network topology by deleting hosts.
			if len(n.deleteHosts) != 0 {
				deleteHostsRequest := &schedulerv2.DeleteHostsRequest{ProbesOfHosts: deleteProbesOfHosts}
				deleteProbesOfHostsRequest := &schedulerv2.SyncNetworkTopologyRequest_DeleteProbesOfHostsRequest{
					DeleteProbesOfHostsRequest: deleteHostsRequest,
				}
				err := schedulerSyncNetworkTopologyClient.Send(&schedulerv2.SyncNetworkTopologyRequest{Request: deleteProbesOfHostsRequest})
				if err != nil {
					return err
				}
				n.deleteHosts = make([]string, 0)
			}
		}
	}
	return nil
}

func (n *networkTopology) Serve() error {

	if err := n.SyncNetworkTopology(); err != nil {
		return err
	}
	tick := time.NewTicker(config.DefaultNetworkTopologySyncInterval)
	for {
		select {
		case <-tick.C:
			if err := n.SyncNetworkTopology(); err != nil {
				return err
			}
		case <-n.done:
			return nil
		}
		return nil
	}
}

func (n *networkTopology) Stop() error {
	close(n.done)
	return nil
}
