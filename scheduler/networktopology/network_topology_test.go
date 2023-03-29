package networktopology

import (
	"reflect"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
)

func Test_NewNetworkTopology(t *testing.T) {
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
				assert.Equal(reflect.TypeOf(n).Elem().Name(), "networkTopology")
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
		config *config.Config
		mock   func(res *resource.MockResource)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name:   "get host",
			config: config.New(),
			mock: func(res *resource.MockResource) {
				res.HostManager().Store(mockHost)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				host, ok := networkTopology.GetHost(mockHost.ID)
				assert.Equal(ok, true)
				assert.EqualValues(host, mockHost)
			},
		},
		{
			name:   "host does not exist",
			config: config.New(),
			mock:   func(res *resource.MockResource) {},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				host, ok := networkTopology.GetHost(mockHost.ID)
				assert.Equal(ok, false)
				assert.Nil(host)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			tc.mock(res)

			n, err := NewNetworkTopology(tc.config, res, mockManagerClient, WithTransportCredentials(nil))
			if err != nil {
				t.Fatal(err)
			}
			tc.expect(t, n)
		})
	}
}

func TestNetworkTopology_LoadParents(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name:   "load parents",
			config: config.New(),
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(config.NetworkTopology.Probe.QueueLength, mockHost)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				m.Store(mockHost.ID, probes)
				networkTopology.StoreParents(mockSeedHost.ID, m)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, ok := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(ok, true)
				value, ok := parents.Load(mockHost.ID)
				assert.Equal(ok, true)

				probes, loaded := value.(*probes)
				assert.Equal(loaded, true)
				assert.EqualValues(probes.host, mockHost)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name:   "parents does not exist",
			config: config.New(),
			mock:   func(networkTopology NetworkTopology, config *config.Config) {},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, loaded := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(loaded, false)
				assert.Nil(parents)
			},
		},
		{
			name:   "load key is empty",
			config: config.New(),
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(config.NetworkTopology.Probe.QueueLength, mockHost)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				m.Store(mockHost.ID, probes)
				networkTopology.StoreParents("", m)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, ok := networkTopology.LoadParents("")
				assert.Equal(ok, true)
				value, ok := parents.Load(mockHost.ID)
				assert.Equal(ok, true)

				probes, loaded := value.(*probes)
				assert.Equal(loaded, true)
				assert.EqualValues(probes.host, mockHost)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
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
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, tc.config)
			tc.expect(t, n)
		})
	}
}

func TestNewNetworkTopology_StoreParents(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name:   "store parents",
			config: config.New(),
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(config.NetworkTopology.Probe.QueueLength, mockHost)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				m.Store(mockHost.ID, probes)
				networkTopology.StoreParents(mockSeedHost.ID, m)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, ok := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(ok, true)
				value, ok := parents.Load(mockHost.ID)
				assert.Equal(ok, true)

				probes, loaded := value.(*probes)
				assert.Equal(loaded, true)
				assert.EqualValues(probes.host, mockHost)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name:   "store key is empty",
			config: config.New(),
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(config.NetworkTopology.Probe.QueueLength, mockHost)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				m.Store(mockHost.ID, probes)
				networkTopology.StoreParents("", m)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, ok := networkTopology.LoadParents("")
				assert.Equal(ok, true)
				value, ok := parents.Load(mockHost.ID)
				assert.Equal(ok, true)

				probes, loaded := value.(*probes)
				assert.Equal(loaded, true)
				assert.EqualValues(probes.host, mockHost)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
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
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, tc.config)
			tc.expect(t, n)
		})
	}
}

func TestNetworkTopology_DeleteParents(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name:   "delete parents",
			config: config.New(),
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(config.NetworkTopology.Probe.QueueLength, mockHost)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				m.Store(mockHost.ID, probes)
				networkTopology.StoreParents(mockSeedHost.ID, m)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				networkTopology.DeleteParents(mockSeedHost.ID)
				parents, ok := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(ok, false)
				assert.Nil(parents)
			},
		},
		{
			name:   "delete key does not exist",
			config: config.New(),
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(config.NetworkTopology.Probe.QueueLength, mockHost)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				m.Store(mockHost.ID, probes)
				networkTopology.StoreParents(mockSeedHost.ID, m)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				networkTopology.DeleteParents(mockHost.ID)
				parents, ok := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(ok, true)
				value, ok := parents.Load(mockHost.ID)
				assert.Equal(ok, true)

				probes, loaded := value.(*probes)
				assert.Equal(loaded, true)
				assert.EqualValues(probes.host, mockHost)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
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
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, tc.config)
			tc.expect(t, n)
		})
	}
}
