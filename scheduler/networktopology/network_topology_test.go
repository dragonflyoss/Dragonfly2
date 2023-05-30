package networktopology

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

func Test_NewNetworkTopology(t *testing.T) {
	tests := []struct {
		name   string
		config config.NetworkTopologyConfig
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "new network topology",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(n).Elem().Name(), "networkTopology")
				assert.Nil(err)
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
			n, err := NewNetworkTopology(tc.config, rdb, res, storage)
			tc.expect(t, n, err)
		})
	}
}

func TestNetworkTopology_LoadDestHostIDs(t *testing.T) {
	tests := []struct {
		name   string
		config config.NetworkTopologyConfig
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "load one destination host",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectKeys(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(
					[]string{fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)},
				)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				destHostIDs, err := n.LoadDestHostIDs(mockSeedHost.ID)
				assert.Nil(err)
				assert.Equal(destHostIDs[0], fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID))
				assert.Equal(len(destHostIDs), 1)
			},
		},
		{
			name: "load destination hosts error",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectKeys(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetErr(errors.New("destination hosts do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				destHostIDs, err := n.LoadDestHostIDs(mockSeedHost.ID)
				assert.Error(err)
				assert.Equal(len(destHostIDs), 0)
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
			n, err := NewNetworkTopology(tc.config, rdb, res, storage)
			tc.expect(t, n, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_DeleteHost(t *testing.T) {
	tests := []struct {
		name   string
		config config.NetworkTopologyConfig
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "delete host",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockSeedHost.ID)).SetVal(1)
				mockRDBClient.ExpectDecrBy(pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID), 1).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				assert.Nil(n.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "delete network topology error",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetErr(errors.New("delete network topology error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				assert.Error(n.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "delete probes which sent by host error",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetErr(errors.New("delete probes which sent by host error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				assert.Error(n.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "delete probes which sent to host error",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockSeedHost.ID)).SetErr(errors.New("delete probes which sent to host error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				assert.Error(n.DeleteHost(mockSeedHost.ID))
			},
		},
		{
			name: "decrease probed count error",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.MatchExpectationsInOrder(true)
				mockRDBClient.ExpectDel(pkgredis.MakeNetworkTopologyKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler(mockSeedHost.ID, "*")).SetVal(1)
				mockRDBClient.ExpectDel(pkgredis.MakeProbesKeyInScheduler("*", mockSeedHost.ID)).SetVal(1)
				mockRDBClient.ExpectDecrBy(pkgredis.MakeProbedCountKeyInScheduler(mockSeedHost.ID), 1).SetErr(errors.New("decrease probed count error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				assert.Error(n.DeleteHost(mockSeedHost.ID))
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
			n, err := NewNetworkTopology(tc.config, rdb, res, storage)
			tc.expect(t, n, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_ProbedCount(t *testing.T) {
	tests := []struct {
		name   string
		config config.NetworkTopologyConfig
		mock   func(mockRDBClient redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "get probed count of host",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetVal("1")
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				probedCount, err := n.ProbedCount(mockHost.ID)
				assert.Equal(probedCount, uint64(1))
				assert.Nil(err)
			},
		},
		{
			name: "get probed count of host error",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			mock: func(mockRDBClient redismock.ClientMock) {
				mockRDBClient.ExpectGet(pkgredis.MakeProbedCountKeyInScheduler(mockHost.ID)).SetErr(errors.New("host do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				probedCount, err := n.ProbedCount(mockHost.ID)
				assert.Equal(probedCount, uint64(0))
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
			n, err := NewNetworkTopology(tc.config, rdb, res, storage)
			tc.expect(t, n, err)
			mockRDBClient.ClearExpect()
		})
	}
}

func TestNetworkTopology_LoadProbes(t *testing.T) {
	tests := []struct {
		name   string
		config config.NetworkTopologyConfig
		expect func(t *testing.T, probes Probes)
	}{
		{
			name: "load probes",
			config: config.NetworkTopologyConfig{
				Enable:          true,
				CollectInterval: 2 * time.Hour,
				Probe: config.ProbeConfig{
					QueueLength: 5,
					Interval:    15 * time.Minute,
					Count:       10,
				},
			},
			expect: func(t *testing.T, probes Probes) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(probes).Elem().Name(), "probes")
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
			n, err := NewNetworkTopology(tc.config, rdb, res, storage)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, n.LoadProbes(mockSeedHost.ID, mockHost.ID))
		})
	}
}
