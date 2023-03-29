package networktopology

import (
	"sync"

	"google.golang.org/grpc/credentials"

	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type NetworkTopology interface {
	// GetHost returns host from host id.
	GetHost(hostID string) (*resource.Host, bool)

	// LoadParents returns parents based on the source host id.
	LoadParents(string) (*sync.Map, bool)

	// StoreParents stores parents.
	StoreParents(key string, parents *sync.Map)

	// DeleteParents deletes parents based on the source host id.
	DeleteParents(string)

	// LoadEdge returns edge between tow hosts.
	LoadEdge(src, dest string) (*Probes, bool)

	// StoreEdge stores edge between two hosts.
	StoreEdge(src, dest string, probes *Probes) bool

	// DeleteEdge deletes edge between two hosts.
	DeleteEdge(src, dest string) bool
}

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
func NewNetworkTopology(cfg *config.Config, resource resource.Resource, managerClient managerclient.V2, options ...Option) (NetworkTopology, error) {
	n := &networkTopology{
		config:        cfg,
		Map:           &sync.Map{},
		resource:      resource,
		managerClient: managerClient,
	}

	for _, opt := range options {
		opt(n)
	}

	return n, nil
}

func (n *networkTopology) GetHost(hostID string) (*resource.Host, bool) {
	host, ok := n.resource.HostManager().Load(hostID)
	if !ok {
		return nil, ok
	}
	return host, ok
}

func (n *networkTopology) LoadParents(key string) (*sync.Map, bool) {
	value, ok := n.Map.Load(key)
	if !ok {
		return nil, ok
	}

	parents, loaded := value.(*sync.Map)
	if !loaded {
		return nil, loaded
	}

	return parents, ok && loaded
}

func (n *networkTopology) StoreParents(key string, parents *sync.Map) {
	n.Map.Store(key, parents)
}

func (n *networkTopology) DeleteParents(key string) {
	n.Map.Delete(key)
}

func (n *networkTopology) LoadEdge(src, dest string) (*Probes, bool) {
	parents, ok := n.Map.Load(src)
	if !ok {
		return nil, ok
	}

	edge, ok := parents.(*sync.Map).Load(dest)
	if !ok {
		return nil, ok
	}

	probes, loaded := edge.(*Probes)
	if !loaded {
		return nil, loaded
	}

	return probes, ok && loaded
}

func (n *networkTopology) StoreEdge(src, dest string, probes *Probes) bool {
	value, ok := n.Map.Load(src)
	if !ok {
		return ok
	}

	parents, loaded := value.(*sync.Map)
	if !loaded {
		return loaded
	}

	parents.Store(dest, probes)
	return ok && loaded

}

func (n *networkTopology) DeleteEdge(src, dest string) bool {
	value, ok := n.Map.Load(src)
	if !ok {
		return ok
	}

	parents, loaded := value.(*sync.Map)
	if !loaded {
		return loaded
	}

	parents.Delete(dest)
	return ok && loaded
}
