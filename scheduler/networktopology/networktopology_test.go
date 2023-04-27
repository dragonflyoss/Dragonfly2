/*
 *     Copyright 2023 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

func Test_NewNetworkTopology(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "new network topology",
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
			storage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), res, storage, mockManagerClient)
			tc.expect(t, n, err)
		})
	}
}

func TestNetworkTopology_LoadParents(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name: "load parents",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
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
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name: "parents does not exist",
			mock: func(networkTopology NetworkTopology, config *config.Config) {},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, loaded := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(loaded, false)
				assert.Nil(parents)
			},
		},
		{
			name: "load key is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
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
			storage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), res, storage, mockManagerClient)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, config.New())
			tc.expect(t, n)
		})
	}
}

func TestNewNetworkTopology_StoreParents(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name: "store parents",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
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
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name: "store key is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
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
			storage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), res, storage, mockManagerClient)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, config.New())
			tc.expect(t, n)
		})
	}
}

func TestNetworkTopology_DeleteParents(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name: "delete parents",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
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
			name: "delete key is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
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
				networkTopology.DeleteParents("")
				parents, ok := networkTopology.LoadParents("")
				assert.Equal(ok, false)
				assert.Nil(parents)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), res, storage, mockManagerClient)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, config.New())
			tc.expect(t, n)
		})
	}
}

func TestNewNetworkTopology_LoadProbes(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name: "load probes",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}
				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
				networkTopology.StoreProbes(mockSeedHost.ID, mockHost.ID, probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				p, ok := networkTopology.LoadProbes(mockSeedHost.ID, mockHost.ID)
				assert.Equal(ok, true)

				probes := p.(*probes)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name: "parents does not exist",
			mock: func(networkTopology NetworkTopology, config *config.Config) {},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, loaded := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(loaded, false)
				assert.Nil(parents)

				edge, loaded := networkTopology.LoadProbes(mockSeedHost.ID, mockHost.ID)
				assert.Equal(loaded, false)
				assert.Nil(edge)
			},
		},
		{
			name: "parents exists but probes does not exist",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				parents, loaded := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(loaded, true)
				assert.NotNil(parents)

				edge, loaded := networkTopology.LoadProbes(mockSeedHost.ID, mockHost.ID)
				assert.Equal(loaded, false)
				assert.Nil(edge)
			},
		},
		{
			name: "load source key is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				networkTopology.StoreParents("", m)
				networkTopology.StoreProbes("", mockHost.ID, probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				p, ok := networkTopology.LoadProbes("", mockHost.ID)
				assert.Equal(ok, true)

				probes := p.(*probes)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name: "load destination key is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
				networkTopology.StoreProbes(mockSeedHost.ID, "", probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				p, ok := networkTopology.LoadProbes(mockSeedHost.ID, "")
				assert.Equal(ok, true)

				probes := p.(*probes)
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
			storage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), res, storage, mockManagerClient)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, config.New())
			tc.expect(t, n)
		})
	}
}

func TestNewNetworkTopology_StoreProbes(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name: "store probes",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}
				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
				networkTopology.StoreProbes(mockSeedHost.ID, mockHost.ID, probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				p, ok := networkTopology.LoadProbes(mockSeedHost.ID, mockHost.ID)
				assert.Equal(ok, true)

				probes := p.(*probes)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name: "store source key is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				networkTopology.StoreParents("", m)
				networkTopology.StoreProbes("", mockHost.ID, probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				p, ok := networkTopology.LoadProbes("", mockHost.ID)
				assert.Equal(ok, true)

				probes := p.(*probes)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name: "store destination key is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
				networkTopology.StoreProbes(mockSeedHost.ID, "", probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				p, ok := networkTopology.LoadProbes(mockSeedHost.ID, "")
				assert.Equal(ok, true)

				probes := p.(*probes)
				assert.Equal(probes.Length(), 1)
				probe, ok := probes.Peek()
				assert.Equal(ok, true)
				assert.EqualValues(probe.Host, mockProbe.Host)
			},
		},
		{
			name: "store probes is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
				networkTopology.StoreProbes(mockSeedHost.ID, mockHost.ID, nil)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				p, ok := networkTopology.LoadProbes(mockSeedHost.ID, mockHost.ID)
				assert.Equal(ok, false)
				assert.Nil(p)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), res, storage, mockManagerClient)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, config.New())
			tc.expect(t, n)
		})
	}
}

func TestNetworkTopology_DeleteProbes(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(networkTopology NetworkTopology, config *config.Config)
		expect func(t *testing.T, networkTopology NetworkTopology)
	}{
		{
			name: "delete parents",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
				networkTopology.StoreProbes(mockSeedHost.ID, mockHost.ID, probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				ok := networkTopology.DeleteProbes(mockSeedHost.ID, mockHost.ID)
				assert.Equal(ok, true)
				parents, ok := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(ok, true)
				assert.NotNil(parents)

				p, ok := networkTopology.LoadProbes(mockSeedHost.ID, mockHost.ID)
				assert.Equal(ok, false)
				assert.Nil(p)
			},
		},
		{
			name: "delete source host is empty",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				networkTopology.StoreParents("", m)
				networkTopology.StoreProbes("", mockHost.ID, probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				ok := networkTopology.DeleteProbes("", mockHost.ID)
				assert.Equal(ok, true)
				parents, ok := networkTopology.LoadParents("")
				assert.Equal(ok, true)
				assert.NotNil(parents)

				p, ok := networkTopology.LoadProbes("", mockHost.ID)
				assert.Equal(ok, false)
				assert.Nil(p)
			},
		},
		{
			name: "delete destination host does not exist",
			mock: func(networkTopology NetworkTopology, config *config.Config) {
				probes := NewProbes(mockQueueLength)
				err := probes.Enqueue(mockProbe)
				if err != nil {
					return
				}

				m := &sync.Map{}
				networkTopology.StoreParents(mockSeedHost.ID, m)
				networkTopology.StoreProbes(mockSeedHost.ID, "", probes)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology) {
				assert := assert.New(t)
				ok := networkTopology.DeleteProbes(mockSeedHost.ID, "")
				assert.Equal(ok, true)
				parents, ok := networkTopology.LoadParents(mockSeedHost.ID)
				assert.Equal(ok, true)
				assert.NotNil(parents)

				p, ok := networkTopology.LoadProbes(mockSeedHost.ID, "")
				assert.Equal(ok, false)
				assert.Nil(p)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), res, storage, mockManagerClient)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n, config.New())
			tc.expect(t, n)
		})
	}
}
