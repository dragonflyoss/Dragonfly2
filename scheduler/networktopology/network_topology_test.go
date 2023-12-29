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
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/pkg/cache"
	"d7y.io/dragonfly/v2/pkg/container/set"
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
			cache := cache.NewMockCache(ctl)
			storage := storagemocks.NewMockStorage(ctl)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.expect(t, networkTopology, err)
		})
	}
}

func TestNetworkTopology_Serve(t *testing.T) {
	tests := []struct {
		name  string
		sleep func()
		mock  func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
			mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "start network topology server",
			sleep: func() {
				time.Sleep(3 * time.Second)
			},
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mc.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any()),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(mockNetworkTopology, mockCacheExpiration, true).Times(2),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go networkTopology.Serve()
			},
		},
		{
			name: "start network topology server error",
			sleep: func() {
				time.Sleep(5 * time.Second)
			},
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetErr(
					errors.New("get probed count keys error"))
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mc.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any()),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(mockNetworkTopology, mockCacheExpiration, true).Times(2),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go networkTopology.Serve()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			cache := cache.NewMockCache(ctl)
			tc.mock(res.EXPECT(), hostManager, hostManager.EXPECT(), storage.EXPECT(), mockRDBClient, cache.EXPECT())

			mockNetworkTopologyConfig.CollectInterval = 2 * time.Second
			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.expect(t, networkTopology, err)
			tc.sleep()
			networkTopology.Stop()
		})
	}
}

func TestNetworkTopology_Has(t *testing.T) {
	tests := []struct {
		name string
		mock func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder)
		run  func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "network topology cache between src host and destination host exists",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, true)
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(networkTopology.Has(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "check network topology between src host and destination host exist error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(
					errors.New("check network topology between src host and destination host exist error"))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.False(networkTopology.Has(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "network topology between src host and destination host exist",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				mockCache.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any())
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.True(networkTopology.Has(mockSeedHost.ID, mockHost.ID))
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
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT())

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.run(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_Store(t *testing.T) {
	tests := []struct {
		name string
		mock func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder)
		run  func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "network topology between src host and destination host exists",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, true)
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Store(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "network topology between src host and destination host does not exist",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(map[string]string{})
				mockRDBClient.Regexp().ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "createdAt", `.*`).SetVal(1)
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Store(mockSeedHost.ID, mockHost.ID))
			},
		},
		{
			name: "set createdAt error when network topology between src host and destination host does not exist",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockCache.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(map[string]string{})
				mockRDBClient.Regexp().ExpectHSet(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), "createdAt", `.*`).SetErr(errors.New("set createdAt error"))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualError(networkTopology.Store(mockSeedHost.ID, mockHost.ID), "set createdAt error")
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
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT())

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.run(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_FindProbedHosts(t *testing.T) {
	tests := []struct {
		name  string
		hosts []*resource.Host
		mock  func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
			mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host)
		expect func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host)
	}{
		{
			name: "find probed hosts with cache",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(uint64(6), mockCacheExpiration, true).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(5),
					mc.Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(5),
				)

				var probedCountKeys []string
				for i := 1; i < len(hosts); i++ {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(hosts[i].ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{"5", "4", "3", "2", "1"})
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.NoError(err)
				assert.Equal(len(probedHosts), 5)
				assert.EqualValues(probedHosts[0].ID, "bac")
				assert.EqualValues(probedHosts[1].ID, "bav")
				assert.EqualValues(probedHosts[2].ID, "baz")
				assert.EqualValues(probedHosts[3].ID, "bar")
				assert.EqualValues(probedHosts[4].ID, "foo")
			},
		},
		{
			name: "find probed hosts",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(6),
					mc.Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(6),
				)

				var probedCountKeys []string
				for _, host := range hosts {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{"6", "5", "4", "3", "2", "1"})
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.NoError(err)
				assert.Equal(len(probedHosts), 5)
				assert.EqualValues(probedHosts[0].ID, "bac")
				assert.EqualValues(probedHosts[1].ID, "bav")
				assert.EqualValues(probedHosts[2].ID, "baz")
				assert.EqualValues(probedHosts[3].ID, "bar")
				assert.EqualValues(probedHosts[4].ID, "foo")
			},
		},
		{
			name:  "find probed hosts when map is insufficient",
			hosts: []*resource.Host{mockHost},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
				)

				var probedCountKeys []string
				for _, host := range hosts {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{"1"})
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.NoError(err)
				assert.Equal(len(probedHosts), 1)
				assert.EqualValues(probedHosts[0].ID, mockHost.ID)
			},
		},
		{
			name: "get probed count error",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(6),
				)

				var probedCountKeys []string
				for _, host := range hosts {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetErr(errors.New("get probed count error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)

				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.Equal(len(probedHosts), 0)
				assert.EqualError(err, "get probed count error")
			},
		},
		{
			name:  "probed hosts not found",
			hosts: []*resource.Host{},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)

				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.Equal(len(probedHosts), 0)
				assert.EqualError(err, "probed hosts not found")
			},
		},
		{
			name: "invalid probed count",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(6),
				)

				var probedCountKeys []string
				for _, host := range hosts {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{"foo", "5", "4", "3", "2", "1"})
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.Equal(len(probedHosts), 0)
				assert.EqualError(err, "invalid probed count")
			},
		},
		{
			name: "invalid value type",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(6),
				)

				var probedCountKeys []string
				for _, host := range hosts {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{6, "5", "4", "3", "2", "1"})
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.Equal(len(probedHosts), 0)
				assert.EqualError(err, "invalid value type")
			},
		},
		{
			name: "type convert error",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(5),
					mc.GetWithExpiration(gomock.Any()).Return("foo", mockCacheExpiration, true).Times(1),
					mc.Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(5),
				)

				var probedCountKeys []string
				for _, host := range hosts[:len(hosts)-1] {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{"6", "5", "4", "3", "2"})
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.NoError(err)
				assert.Equal(len(probedHosts), 5)
				assert.EqualValues(probedHosts[0].ID, "bac")
				assert.EqualValues(probedHosts[1].ID, "bav")
				assert.EqualValues(probedHosts[2].ID, "baz")
				assert.EqualValues(probedHosts[3].ID, "bar")
				assert.EqualValues(probedHosts[4].ID, "foo")
			},
		},
		{
			name: "Initialize the probedCount value of host in redis",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(6),
					mc.Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(5),
				)

				var probedCountKeys []string
				for _, host := range hosts {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{nil, "5", "4", "3", "2", "1"})
				mockRDBClient.ExpectSet(probedCountKeys[0], 0, 0).SetVal("ok")
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.NoError(err)
				assert.Equal(len(probedHosts), 5)
				assert.EqualValues(probedHosts[0].ID, mockHost.ID)
				assert.EqualValues(probedHosts[1].ID, "bac")
				assert.EqualValues(probedHosts[2].ID, "bav")
				assert.EqualValues(probedHosts[3].ID, "baz")
				assert.EqualValues(probedHosts[4].ID, "bar")
			},
		},
		{
			name: "Initialize the probedCount value of host in redis error",
			hosts: []*resource.Host{
				mockHost, {ID: "foo"}, {ID: "bar"}, {ID: "baz"}, {ID: "bav"}, {ID: "bac"},
			},
			mock: func(mockRDBClient redismock.ClientMock, mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, mc *cache.MockCacheMockRecorder, hosts []*resource.Host) {
				mockRDBClient.MatchExpectationsInOrder(true)
				blocklist := set.NewSafeSet[string]()
				blocklist.Add(mockSeedHost.ID)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.LoadRandomHosts(gomock.Eq(findProbedCandidateHostsLimit), gomock.Eq(blocklist)).Return(hosts).Times(1),
					mc.GetWithExpiration(gomock.Any()).Return(nil, mockCacheExpiration, false).Times(6),
				)

				var probedCountKeys []string
				for _, host := range hosts {
					probedCountKeys = append(probedCountKeys, pkgredis.MakeProbedCountKeyInScheduler(host.ID))
				}

				mockRDBClient.ExpectMGet(probedCountKeys...).SetVal([]any{nil, "5", "4", "3", "2", "1"})
				mockRDBClient.ExpectSet(probedCountKeys[0], 0, 0).SetErr(errors.New("Initialize the probedCount value of host in redis error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error, hosts []*resource.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probedHosts, err := networkTopology.FindProbedHosts(mockSeedHost.ID)
				assert.Equal(len(probedHosts), 0)
				assert.EqualError(err, "Initialize the probedCount value of host in redis error")
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
			hostManager := resource.NewMockHostManager(ctl)
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, res.EXPECT(), hostManager, hostManager.EXPECT(), cache.EXPECT(), tc.hosts)

			mockNetworkTopologyConfig.Probe.Count = 5
			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.expect(t, networkTopology, err, tc.hosts)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_DeleteHost(t *testing.T) {
	tests := []struct {
		name       string
		deleteKeys []string
		mock       func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, keys []string)
		run        func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "delete host",
			deleteKeys: []string{pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID),
				pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID),
				pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, keys []string) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetVal([]string{keys[1]}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetVal([]string{keys[2]}, 0)
				mockRDBClient.ExpectDel(keys...).SetVal(4)
				mockCache.Delete(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID))
				mockCache.Delete(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID))
				mockCache.Delete(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.DeleteHost(mockHost.ID))
			},
		},
		{
			name:       "get source network topology keys error",
			deleteKeys: []string{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, keys []string) {
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetErr(
					errors.New("get source network topology keys error"))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualError(networkTopology.DeleteHost(mockHost.ID), "get source network topology keys error")
			},
		},
		{
			name:       "get destination network topology keys error",
			deleteKeys: []string{},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, keys []string) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetErr(
					errors.New("get destination network topology keys error"))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualError(networkTopology.DeleteHost(mockHost.ID), "get destination network topology keys error")
			},
		},
		{
			name:       "get source probes keys error",
			deleteKeys: []string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, keys []string) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetVal([]string{keys[0]}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetErr(
					errors.New("get source probes keys error"))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualError(networkTopology.DeleteHost(mockHost.ID), "get source probes keys error")
			},
		},
		{
			name:       "get destination probes keys error",
			deleteKeys: []string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, keys []string) {
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetVal([]string{keys[0]}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetErr(
					errors.New("get destination probes keys error"))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualError(networkTopology.DeleteHost(mockHost.ID), "get destination probes keys error")
			},
		},
		{
			name: "delete network topology and probes error",
			deleteKeys: []string{pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID),
				pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID),
				pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, mockHost.ID)},
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder, keys []string) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetVal([]string{keys[1]}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbesKeyInScheduler(mockHost.ID, "*"), math.MaxInt64).SetVal([]string{}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbesKeyInScheduler("*", mockHost.ID), math.MaxInt64).SetVal([]string{keys[2]}, 0)
				mockRDBClient.ExpectDel(keys...).SetErr(errors.New("delete network topology and probes error"))
			},
			run: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualError(networkTopology.DeleteHost(mockHost.ID), "delete network topology and probes error")
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
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT(), tc.deleteKeys)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.run(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_Probes(t *testing.T) {
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
				assert.Equal(probes.config.Probe.QueueLength, 5)
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
			cache := cache.NewMockCache(ctl)

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.expect(t, networkTopology, err)
		})
	}
}

func TestNetworkTopology_ProbedCount(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "get probed count with cache",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).Return(uint64(mockProbedCount), mockCacheExpiration, true)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				probedCount, err := networkTopology.ProbedCount(mockHost.ID)
				assert.EqualValues(probedCount, mockProbedCount)
				assert.NoError(err)
			},
		},
		{
			name: "get probed count",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal(strconv.Itoa(mockProbedCount))
				mockCache.Set(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID), uint64(mockProbedCount), gomock.Any())
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				probedCount, err := networkTopology.ProbedCount(mockHost.ID)
				assert.EqualValues(probedCount, mockProbedCount)
				assert.NoError(err)
			},
		},
		{
			name: "type convert error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).Return("foo", mockCacheExpiration, true)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				probedCount, err := networkTopology.ProbedCount(mockHost.ID)
				assert.Equal(probedCount, uint64(0))
				assert.EqualError(err, "get probedCount failed")
			},
		},
		{
			name: "get probed count error",
			mock: func(mockRDBClient redismock.ClientMock, mockCache *cache.MockCacheMockRecorder) {
				mockCache.GetWithExpiration(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).Return(nil, mockCacheExpiration, false)
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetErr(errors.New("get probed count error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)

				probedCount, _ := networkTopology.ProbedCount(mockHost.ID)
				assert.Equal(probedCount, uint64(0))
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
			cache := cache.NewMockCache(ctl)
			tc.mock(mockRDBClient, cache.EXPECT())

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.expect(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_Snapshot(t *testing.T) {
	tests := []struct {
		name string
		mock func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
			mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder)
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "writes the current network topology to the storage",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false).Times(1),
					mc.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any()).Times(1),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(mockNetworkTopology, mockCacheExpiration, true).Times(2),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "get probed count keys error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetErr(
					errors.New("get probed count keys error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualError(networkTopology.Snapshot(), "get probed count keys error")
			},
		},
		{
			name: "parse probed count keys in scheduler error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{"foo"}, 0)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "get network topology keys error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetErr(
					errors.New("get network topology keys error"))
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "parse network topology keys in scheduler error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{"foo"}, 0)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "construct destination hosts for network topology error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(nil, false),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "get averageRTT error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(
					errors.New("get averageRTT error"))
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false).Times(1),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "get createdAt error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(
					errors.New("get createdAt error"))
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mc.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any()),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "get updatedAt error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetErr(
					errors.New("get updatedAt error"))
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mc.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any()),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(mockNetworkTopology, mockCacheExpiration, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "construct source hosts for network topology error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mc.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any()),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(mockNetworkTopology, mockCacheExpiration, true).Times(2),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(nil, false),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
		{
			name: "inserts the network topology into csv file error",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, mockRDBClient redismock.ClientMock, mc *cache.MockCacheMockRecorder) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectScan(0, pkgredis.MakeProbedCountKeyInScheduler("*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID)}, 0)
				mockRDBClient.ExpectScan(0, pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*"), math.MaxInt64).SetVal(
					[]string{pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)}, 0)
				mockRDBClient.ExpectHGetAll(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).SetVal(mockNetworkTopology)
				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(nil, mockCacheExpiration, false),
					mc.Set(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID), mockNetworkTopology, gomock.Any()),
					mc.GetWithExpiration(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, mockHost.ID)).Return(mockNetworkTopology, mockCacheExpiration, true).Times(2),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
					ms.CreateNetworkTopology(gomock.Any()).Return(errors.New("inserts the network topology into csv file error")).Times(1),
				)
			},
			expect: func(t *testing.T, networkTopology NetworkTopology, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.NoError(networkTopology.Snapshot())
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, mockRDBClient := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			hostManager := resource.NewMockHostManager(ctl)
			storage := storagemocks.NewMockStorage(ctl)
			cache := cache.NewMockCache(ctl)
			tc.mock(res.EXPECT(), hostManager, hostManager.EXPECT(), storage.EXPECT(), mockRDBClient, cache.EXPECT())

			networkTopology, err := NewNetworkTopology(mockNetworkTopologyConfig, rdb, cache, res, storage)
			tc.expect(t, networkTopology, err)
			mockRDBClient.ClearExpect()
		})
	}
}
