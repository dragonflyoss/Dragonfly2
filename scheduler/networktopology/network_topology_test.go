package networktopology

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

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
				clientMock.ExpectRPush("probes:"+mockSeedHost.ID+":"+mockHost.ID, data).SetErr(nil)
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 1).SetVal(string(data))
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(1)

				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "averageRTT").SetVal(mockProbe.RTT.String())
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "createdAt").SetVal(mockProbesCreateAt.String())
				clientMock.ExpectHGet("network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "updatedAt").SetVal(mockProbe.CreatedAt.String())
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

				averageRTT, err := n.(*networkTopology).rdb.HGet(context.Background(), "network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "averageRTT").Result()
				a.Equal(averageRTT, mockProbe.RTT.String())
				createdAt, err := n.(*networkTopology).rdb.HGet(context.Background(), "network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "createdAt").Result()
				a.Equal(createdAt, mockProbesCreateAt.String())
				updatedAt, err := n.(*networkTopology).rdb.HGet(context.Background(), "network-topology:"+mockSeedHost.ID+":"+mockHost.ID, "updatedAt").Result()
				a.Equal(updatedAt, mockProbe.CreatedAt.String())
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
