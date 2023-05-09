package networktopology

import (
	"errors"
	"github.com/go-redis/redismock/v8"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	managerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

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
				assert.NotNil(instance.rdb)
				assert.NotNil(instance.config)
				assert.NotNil(instance.resource)
				assert.NotNil(instance.storage)
				assert.NotNil(instance.managerClient)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			n, err := NewNetworkTopology(tc.config, res, mockStorage, mockManagerClient)
			tc.expect(t, n, err)
		})
	}
}

func TestNetworkTopology_Peek(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(n NetworkTopology)
		expect func(t *testing.T, n NetworkTopology)
	}{
		{
			name: "queue has one probe",
			mock: func(n NetworkTopology) {
				rdb, clientMock := redismock.NewClientMock()
				n.(*networkTopology).rdb = rdb

				data, err := mockProbe.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetVal(string(data))
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(1)
			},
			expect: func(t *testing.T, n NetworkTopology) {
				assert := assert.New(t)
				probe, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.True(peeked)
				assert.EqualValues(probe, mockProbe)
				assert.Equal(n.Length(mockSeedHost.ID, mockHost.ID), 1)
			},
		},
		{
			name: "queue has three probes",
			mock: func(n NetworkTopology) {
				rdb, clientMock := redismock.NewClientMock()
				n.(*networkTopology).rdb = rdb

				data, err := mockProbe.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetVal(string(data))

				data1, err := NewProbe(mockHost, 3100000*time.Nanosecond, time.Now()).MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 1).SetVal(string(data1))

				data2, err := NewProbe(mockHost, 3200000*time.Nanosecond, time.Now()).MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 2).SetVal(string(data2))
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(3)
			},
			expect: func(t *testing.T, n NetworkTopology) {
				assert := assert.New(t)
				probe, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.True(peeked)
				assert.EqualValues(probe, mockProbe)
				assert.Equal(n.Length(mockSeedHost.ID, mockHost.ID), 3)
			},
		},
		{
			name: "queue has no probe",
			mock: func(n NetworkTopology) {
				rdb, clientMock := redismock.NewClientMock()
				n.(*networkTopology).rdb = rdb
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetErr(errors.New("no probe"))
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(0)
			},
			expect: func(t *testing.T, n NetworkTopology) {
				assert := assert.New(t)
				_, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.False(peeked)
				assert.Equal(n.Length(mockSeedHost.ID, mockHost.ID), 0)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			mockManagerClient := managerclientmocks.NewMockV2(ctl)
			n, err := NewNetworkTopology(config.New(), res, mockStorage, mockManagerClient)
			if err != nil {
				t.Fatal(err)
			}

			tc.mock(n)
			tc.expect(t, n)
		})
	}
}
