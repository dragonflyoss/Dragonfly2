package networktopology

import (
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

func TestNetworkTopology_Peek(t *testing.T) {
	tests := []struct {
		name   string
		mock   func(clientMock redismock.ClientMock)
		expect func(t *testing.T, n *networkTopology)
	}{
		{
			name: "queue has one probe",
			mock: func(clientMock redismock.ClientMock) {
				data, err := mockProbe.MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetVal(string(data))
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(1)
			},
			expect: func(t *testing.T, n *networkTopology) {
				probe, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.ObjectsAreEqualValues(probe, mockProbe)

				assert := assert.New(t)
				assert.True(peeked)
				assert.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(1))
			},
		},
		{
			name: "queue has three probes",
			mock: func(clientMock redismock.ClientMock) {
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
			expect: func(t *testing.T, n *networkTopology) {
				probe, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.ObjectsAreEqualValues(probe, mockProbe)

				assert := assert.New(t)
				assert.True(peeked)
				assert.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(3))
			},
		},
		{
			name: "queue has no probe",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.ExpectLIndex("probes:"+mockSeedHost.ID+":"+mockHost.ID, 0).SetErr(errors.New("no probe"))
				clientMock.ExpectLLen("probes:" + mockSeedHost.ID + ":" + mockHost.ID).SetVal(0)
			},
			expect: func(t *testing.T, n *networkTopology) {
				assert := assert.New(t)
				_, peeked := n.Peek(mockSeedHost.ID, mockHost.ID)
				assert.False(peeked)
				assert.Equal(n.Length(mockSeedHost.ID, mockHost.ID), int64(0))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			res := resource.NewMockResource(ctl)
			mockStorage := storagemocks.NewMockStorage(ctl)
			rdb, clientMock := redismock.NewClientMock()
			tc.mock(clientMock)
			n := &networkTopology{
				rdb:      rdb,
				config:   config.New(),
				resource: res,
				storage:  mockStorage,
			}

			tc.expect(t, n)
		})
	}
}
