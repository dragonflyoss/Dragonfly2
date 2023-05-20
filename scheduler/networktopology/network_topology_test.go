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
	"d7y.io/dragonfly/v2/scheduler/storage"
	storagemocks "d7y.io/dragonfly/v2/scheduler/storage/mocks"
)

var (
	mockProbesCreatedAt = time.Now()

	mockDestHost = storage.DestHost{
		Host: storage.Host{
			ID:                    mockHost.ID,
			Type:                  mockHost.Type.Name(),
			Hostname:              mockHost.Hostname,
			IP:                    mockHost.IP,
			Port:                  mockHost.Port,
			DownloadPort:          mockHost.DownloadPort,
			OS:                    mockHost.OS,
			Platform:              mockHost.Platform,
			PlatformFamily:        mockHost.PlatformFamily,
			PlatformVersion:       mockHost.PlatformVersion,
			KernelVersion:         mockHost.KernelVersion,
			ConcurrentUploadLimit: int32(300),
			ConcurrentUploadCount: int32(0),
			UploadCount:           int64(0),
			UploadFailedCount:     int64(0),
			CPU: resource.CPU{
				LogicalCount:   mockHost.CPU.LogicalCount,
				PhysicalCount:  mockHost.CPU.PhysicalCount,
				Percent:        mockHost.CPU.Percent,
				ProcessPercent: mockHost.CPU.ProcessPercent,
				Times: resource.CPUTimes{
					User:      mockHost.CPU.Times.User,
					System:    mockHost.CPU.Times.System,
					Idle:      mockHost.CPU.Times.Idle,
					Nice:      mockHost.CPU.Times.Nice,
					Iowait:    mockHost.CPU.Times.Iowait,
					Irq:       mockHost.CPU.Times.Irq,
					Softirq:   mockHost.CPU.Times.Softirq,
					Steal:     mockHost.CPU.Times.Steal,
					Guest:     mockHost.CPU.Times.Guest,
					GuestNice: mockHost.CPU.Times.GuestNice,
				},
			},
			Memory: resource.Memory{
				Total:              mockHost.Memory.Total,
				Available:          mockHost.Memory.Available,
				Used:               mockHost.Memory.Used,
				UsedPercent:        mockHost.Memory.UsedPercent,
				ProcessUsedPercent: mockHost.Memory.ProcessUsedPercent,
				Free:               mockHost.Memory.Free,
			},
			Network: resource.Network{
				TCPConnectionCount:       mockHost.Network.TCPConnectionCount,
				UploadTCPConnectionCount: mockHost.Network.UploadTCPConnectionCount,
				Location:                 mockHost.Network.Location,
				IDC:                      mockHost.Network.IDC,
			},
			Disk: resource.Disk{
				Total:             mockHost.Disk.Total,
				Free:              mockHost.Disk.Free,
				Used:              mockHost.Disk.Used,
				UsedPercent:       mockHost.Disk.UsedPercent,
				InodesTotal:       mockHost.Disk.InodesTotal,
				InodesUsed:        mockHost.Disk.InodesUsed,
				InodesFree:        mockHost.Disk.InodesFree,
				InodesUsedPercent: mockHost.Disk.InodesUsedPercent,
			},
			Build: resource.Build{
				GitVersion: mockHost.Build.GitVersion,
				GitCommit:  mockHost.Build.GitCommit,
				GoVersion:  mockHost.Build.GoVersion,
				Platform:   mockHost.Build.Platform,
			},
			CreatedAt: int64(mockHost.CreatedAt.Load().Nanosecond()),
			UpdatedAt: int64(mockHost.UpdatedAt.Load().Nanosecond()),
		},
		Probes: storage.Probes{
			AverageRTT: int64(mockProbe.RTT),
			CreatedAt:  0,
			UpdatedAt:  0,
		},
	}

	mockID = time.Now().Format(TimeFormat)

	mockNetworkTopology = storage.NetworkTopology{
		ID: mockID,
		Host: storage.Host{
			ID:                    mockSeedHost.ID,
			Type:                  mockSeedHost.Type.Name(),
			Hostname:              mockSeedHost.Hostname,
			IP:                    mockSeedHost.IP,
			Port:                  mockSeedHost.Port,
			DownloadPort:          mockSeedHost.DownloadPort,
			OS:                    mockSeedHost.OS,
			Platform:              mockSeedHost.Platform,
			PlatformFamily:        mockSeedHost.PlatformFamily,
			PlatformVersion:       mockSeedHost.PlatformVersion,
			KernelVersion:         mockSeedHost.KernelVersion,
			ConcurrentUploadLimit: int32(300),
			ConcurrentUploadCount: int32(0),
			UploadCount:           int64(0),
			UploadFailedCount:     int64(0),
			CPU: resource.CPU{
				LogicalCount:   mockSeedHost.CPU.LogicalCount,
				PhysicalCount:  mockSeedHost.CPU.PhysicalCount,
				Percent:        mockSeedHost.CPU.Percent,
				ProcessPercent: mockSeedHost.CPU.ProcessPercent,
				Times: resource.CPUTimes{
					User:      mockSeedHost.CPU.Times.User,
					System:    mockSeedHost.CPU.Times.System,
					Idle:      mockSeedHost.CPU.Times.Idle,
					Nice:      mockSeedHost.CPU.Times.Nice,
					Iowait:    mockSeedHost.CPU.Times.Iowait,
					Irq:       mockSeedHost.CPU.Times.Irq,
					Softirq:   mockSeedHost.CPU.Times.Softirq,
					Steal:     mockSeedHost.CPU.Times.Steal,
					Guest:     mockSeedHost.CPU.Times.Guest,
					GuestNice: mockSeedHost.CPU.Times.GuestNice,
				},
			},
			Memory: resource.Memory{
				Total:              mockSeedHost.Memory.Total,
				Available:          mockSeedHost.Memory.Available,
				Used:               mockSeedHost.Memory.Used,
				UsedPercent:        mockSeedHost.Memory.UsedPercent,
				ProcessUsedPercent: mockSeedHost.Memory.ProcessUsedPercent,
				Free:               mockSeedHost.Memory.Free,
			},
			Network: resource.Network{
				TCPConnectionCount:       mockSeedHost.Network.TCPConnectionCount,
				UploadTCPConnectionCount: mockSeedHost.Network.UploadTCPConnectionCount,
				Location:                 mockSeedHost.Network.Location,
				IDC:                      mockSeedHost.Network.IDC,
			},
			Disk: resource.Disk{
				Total:             mockSeedHost.Disk.Total,
				Free:              mockSeedHost.Disk.Free,
				Used:              mockSeedHost.Disk.Used,
				UsedPercent:       mockSeedHost.Disk.UsedPercent,
				InodesTotal:       mockSeedHost.Disk.InodesTotal,
				InodesUsed:        mockSeedHost.Disk.InodesUsed,
				InodesFree:        mockSeedHost.Disk.InodesFree,
				InodesUsedPercent: mockSeedHost.Disk.InodesUsedPercent,
			},
			Build: resource.Build{
				GitVersion: mockSeedHost.Build.GitVersion,
				GitCommit:  mockSeedHost.Build.GitCommit,
				GoVersion:  mockSeedHost.Build.GoVersion,
				Platform:   mockSeedHost.Build.Platform,
			},
			CreatedAt: int64(mockSeedHost.CreatedAt.Load().Nanosecond()),
			UpdatedAt: int64(mockSeedHost.UpdatedAt.Load().Nanosecond()),
		},
		DestHosts: []storage.DestHost{mockDestHost},
	}
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
				clientMock.MatchExpectationsInOrder(true)
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLIndex(str, 0).SetVal(string(data))

				clientMock.ExpectLLen(str).SetVal(1)
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
				clientMock.MatchExpectationsInOrder(true)
				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(str).SetVal(0)
				clientMock.ExpectLIndex(str, 0).SetErr(errors.New("no probe"))
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
			clientMock.ClearExpect()
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
				clientMock.MatchExpectationsInOrder(true)
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectRPush(str, data).SetVal(1)
				clientMock.ExpectLIndex(str, 0).SetVal(string(data))
				clientMock.ExpectLLen(str).SetVal(1)
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
			clientMock.ClearExpect()
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
				clientMock.MatchExpectationsInOrder(true)
				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}

				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLPop(str).SetVal(string(data))
				clientMock.ExpectLIndex(str, 0).RedisNil()
				clientMock.ExpectLLen(str).SetVal(0)
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
				clientMock.MatchExpectationsInOrder(true)
				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLPop(str).RedisNil()
				clientMock.ExpectLIndex(str, 0).RedisNil()
				clientMock.ExpectLLen(str).SetVal(0)
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
			clientMock.ClearExpect()
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
				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(str).SetVal(1)
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
			clientMock.ClearExpect()
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
				str := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(str, "createdAt").SetVal(mockProbesCreatedAt.Format(TimeFormat))
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
				str := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(str, "createdAt").SetErr(errors.New("probes do not exist"))
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
			clientMock.ClearExpect()
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
				str := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(str, "updatedAt").SetVal(mockHost.CreatedAt.Load().Format(TimeFormat))
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
				str := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(str, "updatedAt").SetErr(errors.New("probes do not exist"))
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
			clientMock.ClearExpect()
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
				str := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(str, "averageRTT").SetVal(mockProbe.RTT.String())
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
				str := fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(str, "averageRTT").SetErr(errors.New("probes do not exist"))
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
			clientMock.ClearExpect()
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
				str := fmt.Sprintf("visitTimes:%s", mockHost.ID)
				clientMock.ExpectGet(str).SetVal("1")
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
				str := fmt.Sprintf("visitTimes:%s", mockHost.ID)
				clientMock.ExpectGet(str).SetErr(errors.New("host do not exist"))
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
				str := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				mockKeys := []string{fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)}
				clientMock.ExpectKeys(str).SetVal(mockKeys)
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
				str := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectKeys(str).SetErr(errors.New("destination hosts do not exist"))
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
				str := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("probes:*:%s", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("visitTimes:%s", mockSeedHost.ID)
				clientMock.ExpectDecrBy(str, 1).SetVal(1)
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
				str := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetErr(errors.New("delete network topology error"))
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
				str := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetErr(errors.New("delete probes which sent by host error"))
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
				str := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("probes:*:%s", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetErr(errors.New("delete probes which sent to host error"))
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
				clientMock.MatchExpectationsInOrder(true)
				str := fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("probes:%s:*", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("probes:*:%s", mockSeedHost.ID)
				clientMock.ExpectDel(str).SetVal(1)

				str = fmt.Sprintf("visitTimes:%s", mockSeedHost.ID)
				clientMock.ExpectDecrBy(str, 1).SetErr(errors.New("delete visit times error"))
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
			name: "store probe when probe list has not element",
			mock: func(clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(str).SetVal(0)

				data, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush(str, data).SetVal(1)

				clientMock.MatchExpectationsInOrder(false)
				str = fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHSet(str, "averageRTT", mockProbe.RTT).SetVal(0)
				clientMock.ExpectHSet(str, "createdAt", mockProbesCreatedAt.Format(TimeFormat)).SetVal(0)
				clientMock.ExpectHSet(str, "updatedAt", mockProbe.CreatedAt.Format(TimeFormat)).SetVal(0)

				clientMock.MatchExpectationsInOrder(true)
				str = fmt.Sprintf("visitTimes:%s", mockHost.ID)
				clientMock.ExpectIncr(str).SetVal(1)
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
				clientMock.MatchExpectationsInOrder(true)
				str := fmt.Sprintf("probes:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectLLen(str).SetVal(5)

				p := NewProbe(mockHost, 3100000*time.Nanosecond, time.Now())
				popData, err := json.Marshal(p)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectLPop(str).SetVal(string(popData))

				pushData, err := json.Marshal(mockProbe)
				if err != nil {
					t.Fatal(err)
				}
				clientMock.ExpectRPush(str, pushData).SetVal(5)

				str = fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)
				clientMock.ExpectHGet(str, "averageRTT").SetVal("3100000")

				clientMock.MatchExpectationsInOrder(false)
				clientMock.ExpectHSet(str, "averageRTT", float64(3010000)).SetVal(0)
				clientMock.ExpectHSet(str, "updatedAt", mockProbe.CreatedAt.Format(TimeFormat)).SetVal(0)

				clientMock.MatchExpectationsInOrder(true)
				str = fmt.Sprintf("visitTimes:%s", mockHost.ID)
				clientMock.ExpectIncr(str).SetVal(1)
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

func TestNetworkTopology_LoadAndCreateNetworkTopology(t *testing.T) {
	tests := []struct {
		name string
		mock func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
			mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, clientMock redismock.ClientMock)
		expect func(t *testing.T, n NetworkTopology, err error)
	}{
		{
			name: "load and create network topology",
			mock: func(mr *resource.MockResourceMockRecorder, hostManager resource.HostManager,
				mh *resource.MockHostManagerMockRecorder, ms *storagemocks.MockStorageMockRecorder, clientMock redismock.ClientMock) {
				clientMock.MatchExpectationsInOrder(true)
				str := fmt.Sprintf("network-topology:*")
				mockKeys := []string{fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)}
				clientMock.ExpectKeys(str).SetVal(mockKeys)

				str = fmt.Sprintf("network-topology:%s:*", mockSeedHost.ID)
				clientMock.ExpectKeys(str).SetVal(mockKeys)

				str = fmt.Sprintf("network-topology:%s:%s", mockSeedHost.ID, mockHost.ID)

				clientMock.ExpectHGet(str, "averageRTT").SetVal(mockProbe.RTT.String())
				clientMock.ExpectHGet(str, "createdAt").SetVal(mockProbesCreatedAt.Format(TimeFormat))
				clientMock.ExpectHGet(str, "updatedAt").SetVal(mockProbe.CreatedAt.Format(TimeFormat))

				gomock.InOrder(
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockHost.ID)).Return(mockHost, true),
					mr.HostManager().Return(hostManager).Times(1),
					mh.Load(gomock.Eq(mockSeedHost.ID)).Return(mockSeedHost, true),
				)

				createdAt, err := time.Parse(TimeFormat, mockProbesCreatedAt.Format(TimeFormat))
				if err != nil {
					t.Fatal(err)
				}
				mockNetworkTopology.DestHosts[0].Probes.CreatedAt = int64(createdAt.Nanosecond())

				updatedAt, err := time.Parse(TimeFormat, mockProbe.CreatedAt.Format(TimeFormat))
				if err != nil {
					t.Fatal(err)
				}
				mockNetworkTopology.DestHosts[0].Probes.UpdatedAt = int64(updatedAt.Nanosecond())

				ms.CreateNetworkTopology(gomock.Eq(mockNetworkTopology)).Return(nil).AnyTimes()
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				a := assert.New(t)
				a.Nil(err)
				err = n.LoadAndCreateNetworkTopology()
				a.Nil(err)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			rdb, clientMock := redismock.NewClientMock()
			res := resource.NewMockResource(ctl)
			hostManager := resource.NewMockHostManager(ctl)

			mockStorage := storagemocks.NewMockStorage(ctl)
			tc.mock(res.EXPECT(), hostManager, hostManager.EXPECT(), mockStorage.EXPECT(), clientMock)
			n, err := NewNetworkTopology(config.New(), rdb, res, mockStorage)
			tc.expect(t, n, err)
			clientMock.ClearExpect()
		})
	}
}
