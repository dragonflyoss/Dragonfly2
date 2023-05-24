package networktopology

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

func Test_NewNetworkTopology(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "new network topology",
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Equal(reflect.TypeOf(n).Elem().Name(), "networkTopology")
				a.Nil(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, _ := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(config.New(), rdb, res, mockStorage)
			tc.expect(t, n, err)
		})
	}
}

func TestNetworkTopology_ProbeCount(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "get probe count of host",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("probe-count:%s", mockHost.ID)
				clientMock.ExpectGet(key).SetVal("1")
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.ProbeCount(mockHost.ID), int64(1))
			},
		},
		{
			name: "get probe count of host error",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("probe-count:%s", mockHost.ID)
				clientMock.ExpectGet(key).SetErr(errors.New("host do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.ProbeCount(mockHost.ID), int64(0))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(clientMock)
			n, err := NewNetworkTopology(config.New(), rdb, res, mockStorage)
			tc.expect(t, n, err)
			clientMock.ClearExpect()
		})
	}
}

func TestNetworkTopology_ProbedCount(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "get probed count of host",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("probed-count:%s", mockHost.ID)
				clientMock.ExpectGet(key).SetVal("1")
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.ProbedCount(mockHost.ID), int64(1))
			},
		},
		{
			name: "get probed count of host error",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("probed-count:%s", mockHost.ID)
				clientMock.ExpectGet(key).SetErr(errors.New("host do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.ProbedCount(mockHost.ID), int64(0))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(clientMock)
			n, err := NewNetworkTopology(config.New(), rdb, res, mockStorage)
			tc.expect(t, n, err)
			clientMock.ClearExpect()
		})
	}
}

func TestNetworkTopology_LoadDestHosts(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "load one destination host",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				mockKeys := []string{fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)}
				clientMock.ExpectKeys(key).SetVal(mockKeys)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				destHosts, ok := n.LoadDestHosts(mockSeedHost.ID)
				a.True(ok)
				a.Equal(destHosts[0], mockHost.ID)
				a.Equal(len(destHosts), 1)
			},
		},
		{
			name: "load destination hosts error",
			mock: func(clientMock redismock.ClientMock) {
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectKeys(key).SetErr(errors.New("destination hosts do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				destHosts, ok := n.LoadDestHosts(mockSeedHost.ID)
				a.False(ok)
				a.Equal(len(destHosts), 0)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(clientMock)
			n, err := NewNetworkTopology(config.New(), rdb, res, mockStorage)
			tc.expect(t, n, err)
			clientMock.ClearExpect()
		})
	}
}

func TestNetworkTopology_DeleteHost(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "delete host",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:*:%s", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probe-count:%s", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probed-count:%s", mockSeedHost.ID)
				clientMock.ExpectDecrBy(key, 1).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.DeleteHost(mockSeedHost.ID)
				a.Nil(err)
			},
		},
		{
			name: "delete network topology error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetErr(errors.New("delete network topology error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.DeleteHost(mockSeedHost.ID)
				a.Error(err)
			},
		},
		{
			name: "delete probes which sent by host error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetErr(errors.New("delete probes which sent by host error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.DeleteHost(mockSeedHost.ID)
				a.Error(err)
			},
		},
		{
			name: "delete probes which sent to host error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:*:%s", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetErr(errors.New("delete probes which sent to host error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.DeleteHost(mockSeedHost.ID)
				a.Error(err)
			},
		},
		{
			name: "delete probe count error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:*:%s", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probe-count:%s", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetErr(errors.New("delete visit times error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.DeleteHost(mockSeedHost.ID)
				a.Error(err)
			},
		},
		{
			name: "decrease probed count error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				key := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probes:*:%s", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probe-count:%s", mockSeedHost.ID)
				clientMock.ExpectDel(key).SetVal(1)

				key = fmt.Sprintf("probe-count:%s", mockSeedHost.ID)
				clientMock.ExpectDecrBy(key, 1).SetErr(errors.New("delete visit times error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.DeleteHost(mockSeedHost.ID)
				a.Error(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(clientMock)
			n, err := NewNetworkTopology(config.New(), rdb, res, mockStorage)
			tc.expect(t, n, err)
			clientMock.ClearExpect()
		})
	}
}

func TestNetworkTopology_StoreProbe(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "store probe when probe queue is null",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)

				probesKey := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(probesKey).SetVal(0)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush(probesKey, data).SetVal(1)

				networkTopologyKey := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.MatchExpectationsInOrder(false)
				clientMock.ExpectHSet(networkTopologyKey, "averageRTT", mockProbe.RTT.Nanoseconds()).SetVal(1)
				clientMock.ExpectHSet(networkTopologyKey, "createdAt", mockProbe.CreatedAt.UnixNano()).SetVal(1)
				clientMock.ExpectHSet(networkTopologyKey, "updatedAt", mockProbe.CreatedAt.UnixNano()).SetVal(1)

				key := fmt.Sprintf("probe-count:%s", mockSeedHost.ID)
				clientMock.ExpectIncr(key).SetVal(1)

				key = fmt.Sprintf("probed-count:%s", mockHost.ID)
				clientMock.ExpectIncr(key).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				ok := n.StoreProbe(mockSeedHost.ID, mockHost.ID, mockProbe)
				a.True(ok)
			},
		},
		{
			name: "store probe when probe list has one elements",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				probesKey := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(probesKey).SetVal(1)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush(probesKey, data).SetVal(1)

				mockData, err := json.Marshal(NewProbe(mockHost, 3100000*time.Nanosecond, time.Now()))
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLRange(probesKey, 0, -1).SetVal([]string{string(mockData), string(data)})

				networkTopologyKey := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.MatchExpectationsInOrder(false)
				clientMock.ExpectHSet(networkTopologyKey, "averageRTT", int64(2979000)).SetVal(1)
				clientMock.ExpectHSet(networkTopologyKey, "updatedAt", mockProbe.CreatedAt.UnixNano()).SetVal(1)

				key := fmt.Sprintf("probe-count:%s", mockSeedHost.ID)
				clientMock.ExpectIncr(key).SetVal(1)

				key = fmt.Sprintf("probed-count:%s", mockHost.ID)
				clientMock.ExpectIncr(key).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				ok := n.StoreProbe(mockSeedHost.ID, mockHost.ID, mockProbe)
				a.True(ok)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(clientMock)
			n, err := NewNetworkTopology(config.New(), rdb, res, mockStorage)
			tc.expect(t, n, err)
			clientMock.ClearExpect()
		})
	}
}
