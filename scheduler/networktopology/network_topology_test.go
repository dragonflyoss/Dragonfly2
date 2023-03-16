package networktopology

import (
	managerclient "d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

var ()

func TestNetworkTopology_NewNetworkTopology(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name:   "new network topology",
			config: config.New(),
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				instance := n.(*networkTopology)
				assert.NotNil(instance.config)
				assert.NotNil(instance.resource)
				assert.NotNil(instance.managerClient)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			n, err := NewNetworkTopology(tc.config, res, mockManagerClient, WithTransportCredentials(nil))
			tc.expect(t, n, err)
		})
	}

}
func TestNetworkTopology_GetHost(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, res resource.Resource, mockManagerClient managerclient.V2, mockHost *resource.Host)
	}{
		{
			name: "load host",
			expect: func(t *testing.T, res resource.Resource, mockManagerClient managerclient.V2, mockHost *resource.Host) {
				assert := assert.New(t)
				res.HostManager().Store(mockHost)
				n, err := NewNetworkTopology(config.New(), res, mockManagerClient, WithTransportCredentials(nil))
				assert.Nil(err)
				host, loaded := n.GetHost(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
		{
			name: "host does not exist",
			expect: func(t *testing.T, res resource.Resource, mockManagerClient managerclient.V2, mockHost *resource.Host) {
				assert := assert.New(t)
				n, err := NewNetworkTopology(config.New(), res, mockManagerClient, WithTransportCredentials(nil))
				assert.Nil(err)
				_, loaded := n.GetHost(mockHost.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name: "load key is empty",
			expect: func(t *testing.T, res resource.Resource, mockManagerClient managerclient.V2, mockHost *resource.Host) {
				assert := assert.New(t)
				mockHost.ID = ""
				res.HostManager().Store(mockHost)
				n, err := NewNetworkTopology(config.New(), res, mockManagerClient, WithTransportCredentials(nil))
				assert.Nil(err)
				host, loaded := n.GetHost(mockHost.ID)
				assert.Equal(loaded, true)
				assert.Equal(host.ID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			mockHost := resource.NewHost(
				mockSrcHost.ID, mockSrcHost.IP, mockSrcHost.Hostname,
				mockSrcHost.Port, mockSrcHost.DownloadPort, mockSrcHost.Type)

			tc.expect(t, res, mockManagerClient, mockHost)
		})
	}
}

func TestNetworkTopology_StoreSyncHost(t *testing.T) {

}

func TestNetworkTopology_DeleteSyncHost(t *testing.T) {

}

func TestNewNetworkTopology_LoadParents(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		expect func(t *testing.T, networkTopology NetworkTopology, host *resource.Host, m *sync.Map)
	}{
		{
			name:   "load parents",
			config: config.New(),
			expect: func(t *testing.T, networkTopology NetworkTopology, host *resource.Host, m *sync.Map) {
				assert := assert.New(t)
				networkTopology.StoreParents(host.ID, m)
				rawParents, loaded := networkTopology.LoadParents(host.ID)
				assert.Equal(loaded, true)
				rawParents.Range(func(key, value interface{}) bool {
					v, ok := m.Load(key)
					assert.Equal(ok, true)
					assert.Equal(key, v.(Probes).Host.ID)
					return true
				})

			},
		},
		{
			name:   "parents does not exist",
			config: config.New(),
			expect: func(t *testing.T, networkTopology NetworkTopology, host *resource.Host, m *sync.Map) {
				assert := assert.New(t)
				_, loaded := networkTopology.LoadParents(host.ID)
				assert.Equal(loaded, false)
			},
		},
		{
			name:   "load key is empty",
			config: config.New(),
			expect: func(t *testing.T, networkTopology NetworkTopology, host *resource.Host, m *sync.Map) {
				assert := assert.New(t)
				host.ID = ""
				networkTopology.StoreParents(host.ID, m)
				rawParents, loaded := networkTopology.LoadParents(host.ID)
				assert.Equal(loaded, true)
				rawParents.Range(func(key, value interface{}) bool {
					v, ok := m.Load(key)
					assert.Equal(ok, true)
					assert.Equal(key, v.(Probes).Host.ID)
					return true
				})

			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			probes := NewProbes(mockSrcHost)
			probes.StoreProbe(mockProbe)
			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			n, err := NewNetworkTopology(tc.config, res, mockManagerClient, WithTransportCredentials(nil))
			if err != nil {
				t.Fatal(err)
			}
			var m sync.Map
			m.Store(mockProbe.Host.ID, mockProbe)
			tc.expect(t, n, mockSrcHost, &m)
		})
	}
}

func TestNewNetworkTopology_StoreParents(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		expect func(t *testing.T, networkTopology NetworkTopology, host *resource.Host, m *sync.Map)
	}{
		{
			name:   "store parents",
			config: config.New(),
			expect: func(t *testing.T, networkTopology NetworkTopology, host *resource.Host, m *sync.Map) {
				assert := assert.New(t)
				networkTopology.StoreParents(host.ID, m)
				rawParents, loaded := networkTopology.LoadParents(host.ID)
				assert.Equal(loaded, true)
				rawParents.Range(func(key, value interface{}) bool {
					v, ok := m.Load(key)
					assert.Equal(ok, true)
					assert.Equal(key, v.(Probes).Host.ID)
					return true
				})

			},
		},
		{
			name:   "store key is empty",
			config: config.New(),
			expect: func(t *testing.T, networkTopology NetworkTopology, host *resource.Host, m *sync.Map) {
				assert := assert.New(t)
				host.ID = ""
				networkTopology.StoreParents(host.ID, m)
				rawParents, loaded := networkTopology.LoadParents(host.ID)
				assert.Equal(loaded, true)
				rawParents.Range(func(key, value interface{}) bool {
					v, ok := m.Load(key)
					assert.Equal(ok, true)
					assert.Equal(key, v.(Probes).Host.ID)
					return true
				})

			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			probes := NewProbes(mockSrcHost)
			probes.StoreProbe(mockProbe)
			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			n, err := NewNetworkTopology(tc.config, res, mockManagerClient, WithTransportCredentials(nil))
			if err != nil {
				t.Fatal(err)
			}
			var m sync.Map
			m.Store(mockProbe.Host.ID, mockProbe)
			tc.expect(t, n, mockSrcHost, &m)
		})
	}
}
