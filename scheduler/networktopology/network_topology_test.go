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
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
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
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reflect.TypeOf(networkTopology).Elem().Name(), "networkTopology")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, _ := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
		})
	}
}

func TestNewNetworkTopology_Has(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "network topology between src host and destination host exists",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectExists(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.True(networkTopology.Has(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "network topology between src host and destination host does not exist",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectExists(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.False(networkTopology.Has(mockSeedHost.ID, mockHost.ID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNewNetworkTopology_Store(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "network topology between src host and destination host exists",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectExists(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.NoError(networkTopology.Store(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "network topology between src host and destination host does not exist",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectExists(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)
				mockRDBClient.Regexp().ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "createdAt", `.*`).SetVal(1)
				mockRDBClient.ExpectSet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID), 0, 0).SetVal("ok")
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.NoError(networkTopology.Store(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "set createdAt error when network topology between src host and destination host does not exist",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectExists(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)
				mockRDBClient.Regexp().ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "createdAt", `.*`).SetErr(errors.New("set createdAt error"))
				mockRDBClient.ExpectSet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID), 0, 0).SetVal("ok")
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.Error(networkTopology.Store(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "set probed count error when network topology between src host and destination host does not exist",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectExists(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(0)
				mockRDBClient.Regexp().ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "createdAt", `.*`).SetVal(1)
				mockRDBClient.ExpectSet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID), 0, 0).SetErr(errors.New("set probed count error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.Error(networkTopology.Store(mockSeedHost.ID, mockHost.ID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNewNetworkTopology_DeleteHost(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "delete host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedAtKeyInScheduler(mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.NoError(networkTopology.DeleteHost(mockHost.ID))
			},
		},
		{
			name: "delete network topology error when delete host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*")).SetErr(errors.New("delete network topology error"))
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedAtKeyInScheduler(mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.Error(networkTopology.DeleteHost(mockHost.ID))
			},
		},
		{
			name: "delete probes error when delete host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*")).SetErr(errors.New("delete probes error"))
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedAtKeyInScheduler(mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.Error(networkTopology.DeleteHost(mockHost.ID))
			},
		},
		{
			name: "delete the time of the last probe error when delete host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedAtKeyInScheduler(mockHost.ID)).SetErr(errors.New("delete the time of the last probe error"))
				mockRDBClient.ExpectDel(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(1)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.Error(networkTopology.DeleteHost(mockHost.ID))
			},
		},
		{
			name: "delete probed count error when delete host",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(false)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedAtKeyInScheduler(mockHost.ID)).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetErr(errors.New("delete probed count error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.Error(networkTopology.DeleteHost(mockHost.ID))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNewNetworkTopology_ProbedCount(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "get probed count",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(strconv.Itoa(mockProbedCount))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				probedCount, err := networkTopology.ProbedCount(mockHost.ID)
				assert.NoError(err)
				assert.EqualValues(probedCount, mockProbedCount)
			},
		},
		{
			name: "get probed count error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetErr(errors.New("get probed count error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				_, err = networkTopology.ProbedCount(mockHost.ID)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNewNetworkTopology_ProbedAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "get the time of the last probe",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedAtKeyInScheduler(mockHost.ID)).SetVal(mockProbe.CreatedAt.Format(time.RFC3339Nano))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				probedAt, err := networkTopology.ProbedAt(mockHost.ID)
				assert.NoError(err)
				assert.True(mockProbe.CreatedAt.Equal(probedAt))
			},
		},
		{
			name: "get the time of the last probe error",
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetErr(errors.New("get the time of the last probe error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				_, err = networkTopology.ProbedAt(mockHost.ID)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			tc.mock(mockRDBClient)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNewNetworkTopology_Probes(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "loads probes interface",
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				ps := networkTopology.Probes(mockSeedHost.ID, mockHost.ID)
				probes := ps.(*probes)
				assert.Equal(probes.config.QueueLength, 5)
				assert.NotNil(probes.rdb)
				assert.Equal(probes.srcHostID, mockSeedHost.ID)
				assert.Equal(probes.destHostID, mockHost.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, _ := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			storage := storagemocks.NewMockStorage(ctl)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, res, storage)
			tc.expect(t, networkTopology, err)
		})
	}
}
