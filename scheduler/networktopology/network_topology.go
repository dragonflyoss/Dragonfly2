package networktopology

import (
	"sync"
	
	"google.golang.org/grpc/credentials"

	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

type NetworkTopology interface {
	// LoadParents returns parents for a key.
	LoadParents(string) (*sync.Map, bool)

	// StoreParents stores parents.
	StoreParents(key string, parents *sync.Map)

	// DeleteParents deletes parents for a key.
	DeleteParents(string)

	// LoadProbes returns probes between two hosts.
	LoadProbes(src, dest string) (Probes, bool)

	// StoreProbes stores probes between two hosts.
	StoreProbes(src, dest string, probes Probes) bool

	// DeleteProbes deletes probes between tow hosts.
	DeleteProbes(src, dest string) bool
}

type networkTopology struct {
	// network topology
	*sync.Map

	// Scheduler config.
	config *config.Config

	// Resource interface
	resource resource.Resource

	// Manager client interface
	managerClient managerclient.V2

	// TransportCredentials stores the Authenticator required to set up a client connection.
	transportCredentials credentials.TransportCredentials
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

// LoadParents returns parents for a key.
func (n *networkTopology) LoadParents(key string) (*sync.Map, bool) {
	value, loaded := n.Map.Load(key)
	if !loaded {
		return nil, false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return nil, false
	}

	return parents, true
}

// StoreParents stores parents.
func (n *networkTopology) StoreParents(key string, parents *sync.Map) {
	n.Map.Store(key, parents)
}

// DeleteParents deletes parents for a key.
func (n *networkTopology) DeleteParents(key string) {
	n.Map.Delete(key)
}

// LoadProbes returns probes between two hosts.
func (n *networkTopology) LoadProbes(src, dest string) (Probes, bool) {
	value, loaded := n.Map.Load(src)
	if !loaded {
		return nil, false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return nil, false
	}

	p, loaded := parents.Load(dest)
	if !loaded {
		return nil, false
	}

	probes, ok := p.(*probes)
	if !ok {
		return nil, false
	}

	return probes, true
}

// StoreProbes stores probes between two hosts.
func (n *networkTopology) StoreProbes(src, dest string, probes Probes) bool {
	value, loaded := n.Map.Load(src)
	if !loaded {
		return false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return false
	}

	parents.Store(dest, probes)
	return true

}

// DeleteProbes deletes probes between tow hosts.
func (n *networkTopology) DeleteProbes(src, dest string) bool {
	value, loaded := n.Map.Load(src)
	if !loaded {
		return false
	}

	parents, ok := value.(*sync.Map)
	if !ok {
		return false
	}

	parents.Delete(dest)
	return true
}
