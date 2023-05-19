package networktopology

import (
	"encoding/json"
	"errors"
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

var (
	mockProbesCreatedAt = time.Now()
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

func TestNetworkTopology_Peek(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "queue has one probe",
			mock: func(clientMock redismock.ClientMock) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetVal(string(data))

				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				probe, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.ObjectsAreEqualValues(probe, mockProbe)
				a.True(peeked)
				a.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(1))
			},
		},
		{
			name: "queue has no probe",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(0)
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetErr(errors.New("no probe"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				_, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				a.False(peeked)
				a.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(0))
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
		})
	}
}

func TestNetworkTopology_Enqueue(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "enqueue one probe",
			mock: func(clientMock redismock.ClientMock) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				clientMock.ExpectRPush("probes:"+mockSeedHost.ID+":"+mockHost.ID, data).SetVal(1)
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetVal(string(data))
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.Enqueue(mockSeedHost.ID, mockHost.ID, mockProbe)
				a.Nil(err)

				probe, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.ObjectsAreEqualValues(probe, mockProbe)
				a.True(peeked)
				a.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(1))
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
		})
	}
}

func TestNetworkTopology_Dequeue(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "dequeue probe",
			mock: func(clientMock redismock.ClientMock) {
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				clientMock.ExpectLPop("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(string(data))
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).RedisNil()
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(0)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				probe, ok := n.Dequeue(mockSeedHost.ID, mockHost.ID)
				assert.ObjectsAreEqualValues(probe, mockProbe)
				a.True(ok)

				_, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				a.False(peeked)
				a.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(0))
			},
		},
		{
			name: "dequeue probe from empty probes",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectLPop("probes:" + mockSeedHost.ID + ":" + mockHost.ID).RedisNil()
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).RedisNil()
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(0)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				_, ok := n.Dequeue(mockSeedHost.ID, mockHost.ID)
				a.False(ok)

				_, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				a.False(peeked)
				a.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(0))
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
		})
	}
}

func TestNetworkTopology_Length(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "queue has one probe",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(1))
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
		})
	}
}

func TestNetworkTopology_CreatedAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "get creation time of probes",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "createdAt").SetVal(mockProbesCreatedAt.Format(TimeFormat))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.CreatedAt(mockSeedHost.ID, mockHost.ID).Format(TimeFormat), mockProbesCreatedAt.Format(TimeFormat))
			},
		},
		{
			name: "get creation time of probes error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "createdAt").SetErr(errors.New("probes do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.CreatedAt(mockSeedHost.ID, mockHost.ID).Format(TimeFormat), time.Time{}.Format(TimeFormat))
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
		})
	}
}

func TestNetworkTopology_UpdateAt(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "get update time of probes",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "updatedAt").SetVal(mockHost.CreatedAt.Load().Format(TimeFormat))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.UpdatedAt(mockSeedHost.ID, mockHost.ID).Format(TimeFormat), mockHost.CreatedAt.Load().Format(TimeFormat))
			},
		},
		{
			name: "get update time of probes error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "updatedAt").SetErr(errors.New("probes do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.UpdatedAt(mockSeedHost.ID, mockHost.ID).Format(TimeFormat), time.Time{}.Format(TimeFormat))
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
		})
	}
}

func TestNetworkTopology_AverageRTT(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "get averageRTT of probes",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "averageRTT").SetVal(mockProbe.RTT.String())
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.AverageRTT(mockSeedHost.ID, mockHost.ID), mockProbe.RTT)
			},
		},
		{
			name: "get averageRTT of probes error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "averageRTT").SetErr(errors.New("probes do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.AverageRTT(mockSeedHost.ID, mockHost.ID), time.Duration(0))
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
		})
	}
}

func TestNetworkTopology_VisitTimes(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "get visit times of host",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectGet("visitTimes:" + mockHost.ID).SetVal("1")
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.VisitTimes(mockHost.ID), int64(1))
			},
		},
		{
			name: "get visit times of host error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectGet("visitTimes:" + mockHost.ID).SetErr(errors.New("host do not exist"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				a.Equal(n.VisitTimes(mockHost.ID), int64(0))
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
				clientMock.ExpectKeys("network-topology:" + mockSeedHost.ID + ":*").SetVal([]string{"network-topology:" + mockSeedHost.ID + ":" + mockHost.ID})
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
				clientMock.ExpectKeys("network-topology:" + mockSeedHost.ID + ":*").SetErr(errors.New("destination hosts do not exist"))
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
				clientMock.ExpectDel("network-topology:" + mockSeedHost.ID + ":*").SetVal(1)
				clientMock.ExpectDel("probes:" + mockSeedHost.ID + ":*").SetVal(1)
				clientMock.ExpectDel("probes:*:" + mockSeedHost.ID).SetVal(1)
				clientMock.ExpectDecrBy("visitTimes:"+mockSeedHost.ID, 1).SetVal(1)
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
				clientMock.ExpectDel("network-topology:" + mockSeedHost.ID + ":*").SetErr(errors.New("delete network topology error"))
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
				clientMock.ExpectDel("network-topology:" + mockSeedHost.ID + ":*").SetVal(1)
				clientMock.ExpectDel("probes:" + mockSeedHost.ID + ":*").SetErr(errors.New("delete probes which sent by host error"))
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
				clientMock.ExpectDel("network-topology:" + mockSeedHost.ID + ":*").SetVal(1)
				clientMock.ExpectDel("probes:" + mockSeedHost.ID + ":*").SetVal(1)
				clientMock.ExpectDel("probes:*:" + mockSeedHost.ID).SetErr(errors.New("delete probes which sent to host error"))
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.DeleteHost(mockSeedHost.ID)
				a.Error(err)
			},
		},
		{
			name: "delete visit times error",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectDel("network-topology:" + mockSeedHost.ID + ":*").SetVal(1)
				clientMock.ExpectDel("probes:" + mockSeedHost.ID + ":*").SetVal(1)
				clientMock.ExpectDel("probes:*:" + mockSeedHost.ID).SetVal(1)
				clientMock.ExpectDecrBy("visitTimes:"+mockSeedHost.ID, 1).SetErr(errors.New("delete visit times error"))
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
			name: "store probe when probe list has not element",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(0)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush("probes:"+mockSeedHost.ID+":"+mockHost.ID, data).SetVal(1)

				key := "network-topology:" + mockSeedHost.ID + ":" + mockHost.ID
				clientMock.ExpectHSet(key, "averageRTT", mockProbe.RTT).SetVal(0)
				clientMock.ExpectHSet(key, "createdAt", mockProbesCreatedAt.Format(TimeFormat)).SetVal(0)
				clientMock.ExpectHSet(key, "updatedAt", mockProbe.CreatedAt.Format(TimeFormat)).SetVal(0)

				clientMock.ExpectIncr("visitTimes:" + mockHost.ID).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				ok := n.StoreProbe(mockSeedHost.ID, mockHost.ID, mockProbe)
				a.True(ok)
			},
		},
		{
			name: "store probe when probe list has five elements",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(5)

				p := NewProbe(mockHost, 3100000*time.Nanosecond, time.Now())
				popData, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLPop("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(string(popData))

				pushData, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush("probes:"+mockSeedHost.ID+":"+mockHost.ID, pushData).SetVal(5)

				key := "network-topology:" + mockSeedHost.ID + ":" + mockHost.ID
				clientMock.ExpectHGet(key, "averageRTT").SetVal("3100000")
				clientMock.ExpectHSet(key, "averageRTT", float64(3010000)).SetVal(0)
				clientMock.ExpectHSet(key, "updatedAt", mockProbe.CreatedAt.Format(TimeFormat)).SetVal(0)

				clientMock.ExpectIncr("visitTimes:" + mockHost.ID).SetVal(1)
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
		})
	}
}
