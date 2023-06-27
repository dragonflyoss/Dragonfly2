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

package probe

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	v1 "d7y.io/api/pkg/apis/common/v1"
	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
	schedulerv1mocks "d7y.io/api/pkg/apis/scheduler/v1/mocks"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
	schedulerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
)

var (
	mockDaemonConfig = &config.DaemonOption{
		Host: config.HostOption{
			Location:    mockHostLocation,
			IDC:         mockHostIDC,
			Hostname:    idgen.HostIDV2("127.0.0.1", "bar"),
			AdvertiseIP: net.IPv4(127, 0, 0, 1),
		},
		NetworkTopology: config.NetworkTopologyOption{
			Enable: true,
			Probe: config.ProbeOption{
				Interval: 2 * time.Second,
			},
		},
	}

	mockPort         = 8000
	mockDownloadPort = 8001
	mockHostLocation = "location"
	mockHostIDC      = "idc"

	mockHost = &v1.Host{
		Id:           "foo",
		Ip:           "127.0.0.1",
		Hostname:     idgen.HostIDV2("127.0.0.1", "foo"),
		Port:         int32(mockPort),
		DownloadPort: int32(mockDownloadPort),
		Location:     mockHostLocation,
		Idc:          mockHostIDC,
	}

	mockSeedHost = &v1.Host{
		Id:           "bar",
		Ip:           "127.0.0.1",
		Hostname:     idgen.HostIDV2("127.0.0.1", "bar"),
		Port:         int32(mockPort),
		DownloadPort: int32(mockDownloadPort),
		Location:     mockHostLocation,
		Idc:          mockHostIDC,
	}
)

func Test_NewProbe(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, probe Probe, err error)
	}{
		{
			name: "new probe",
			expect: func(t *testing.T, probe Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reflect.TypeOf(probe).Elem().Name(), "probe")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			schedulerClient := schedulerclientmocks.NewMockV1(ctl)
			probe, err := NewProbe(mockDaemonConfig, mockSeedHost.Id, int32(mockPort), int32(mockDownloadPort), schedulerClient)
			tc.expect(t, probe, err)
		})
	}
}

func TestProbe_Serve(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		sleep    func()
		mock     func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
			ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder)
		expect func(t *testing.T, p Probe, err error)
	}{
		{
			name:     "collect probes and upload probes to scheduler",
			interval: 200 * time.Millisecond,
			sleep: func() {
				time.Sleep(300 * time.Millisecond)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				gomock.InOrder(
					mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
						}})).Return(stream, nil).Times(1),
					ms.Recv().Return(&schedulerv1.SyncProbesResponse{
						Hosts: []*v1.Host{mockHost},
					}, nil).Times(1),
					ms.Send(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.Serve()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			schedulerClient := schedulerclientmocks.NewMockV1(ctl)
			stream := schedulerv1mocks.NewMockScheduler_SyncProbesClient(ctl)
			tc.mock(schedulerClient.EXPECT(), stream, stream.EXPECT())
			mockDaemonConfig.NetworkTopology.Probe.Interval = tc.interval
			probe, err := NewProbe(mockDaemonConfig, mockSeedHost.Id, int32(mockPort), int32(mockDownloadPort), schedulerClient)
			tc.expect(t, probe, err)
			tc.sleep()
			probe.Stop()
		})
	}
}

func TestProbe_uploadProbesToScheduler(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		sleep    func()
		mock     func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
			ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder)
		expect func(t *testing.T, p Probe, err error)
	}{
		{
			name:     "collect probes and send ProbeFinishedRequest",
			interval: 200 * time.Millisecond,
			sleep: func() {
				time.Sleep(300 * time.Millisecond)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				gomock.InOrder(
					mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
						}})).Return(stream, nil).Times(1),
					ms.Recv().Return(&schedulerv1.SyncProbesResponse{
						Hosts: []*v1.Host{mockHost},
					}, nil).Times(1),
					ms.Send(gomock.Any()).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.(*probe).uploadProbesToScheduler()
			},
		},
		{
			name:     "collect fail probes and send ProbeFailedRequest",
			interval: 3 * time.Second,
			sleep: func() {
				time.Sleep(5 * time.Second)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				gomock.InOrder(
					mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
						}})).Return(stream, nil).Times(1),
					ms.Recv().Return(&schedulerv1.SyncProbesResponse{
						Hosts: []*v1.Host{
							{
								Id:           idgen.HostIDV2("172.0.0.1", "foo"),
								Ip:           "172.0.0.1",
								Hostname:     "foo",
								Port:         8003,
								DownloadPort: 8001,
								Location:     "location",
								Idc:          "idc",
							},
						},
					}, nil).Times(1),
					ms.Send(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeFailedRequest{
							ProbeFailedRequest: &schedulerv1.ProbeFailedRequest{
								Probes: []*schedulerv1.FailedProbe{
									{
										Host: &v1.Host{
											Id:           idgen.HostIDV2("172.0.0.1", "foo"),
											Ip:           "172.0.0.1",
											Hostname:     "foo",
											Port:         8003,
											DownloadPort: 8001,
											Location:     "location",
											Idc:          "idc",
										},
										Description: "receive packet failed",
									},
								},
							},
						},
					}).Return(nil).Times(1),
				)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.(*probe).uploadProbesToScheduler()
			},
		},
		{
			name:     "syncProbe error",
			interval: 200 * time.Millisecond,
			sleep: func() {
				time.Sleep(300 * time.Millisecond)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
					Host: mockSeedHost,
					Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
						ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
					}})).Return(nil, errors.New("syncProbe error")).Times(1)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.(*probe).uploadProbesToScheduler()
			},
		},
		{
			name:     "receive error",
			interval: 200 * time.Millisecond,
			sleep: func() {
				time.Sleep(300 * time.Millisecond)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				gomock.InOrder(
					mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
						}})).Return(stream, nil).Times(1),
					ms.Recv().Return(nil, errors.New("receive error")).Times(1),
				)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.(*probe).uploadProbesToScheduler()
			},
		},
		{
			name:     "receive EOF",
			interval: 200 * time.Millisecond,
			sleep: func() {
				time.Sleep(300 * time.Millisecond)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				gomock.InOrder(
					mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
						}})).Return(stream, nil).Times(1),
					ms.Recv().Return(nil, io.EOF).Times(1),
				)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.(*probe).uploadProbesToScheduler()
			},
		},
		{
			name:     "send ProbeFinishedRequest error",
			interval: 200 * time.Millisecond,
			sleep: func() {
				time.Sleep(300 * time.Millisecond)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				gomock.InOrder(
					mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
						}})).Return(stream, nil).Times(1),
					ms.Recv().Return(&schedulerv1.SyncProbesResponse{
						Hosts: []*v1.Host{mockHost},
					}, nil).Times(1),
					ms.Send(gomock.Any()).Return(errors.New("send ProbeFinishedRequest error")).Times(1),
				)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.(*probe).uploadProbesToScheduler()
			},
		},
		{
			name:     "send ProbeFailedRequest error",
			interval: 3 * time.Second,
			sleep: func() {
				time.Sleep(5 * time.Second)
			},
			mock: func(mv *mocks.MockV1MockRecorder, stream *schedulerv1mocks.MockScheduler_SyncProbesClient,
				ms *schedulerv1mocks.MockScheduler_SyncProbesClientMockRecorder) {
				gomock.InOrder(
					mv.SyncProbes(gomock.Eq(context.Background()), gomock.Eq(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeStartedRequest{
							ProbeStartedRequest: &schedulerv1.ProbeStartedRequest{},
						}})).Return(stream, nil).Times(1),
					ms.Recv().Return(&schedulerv1.SyncProbesResponse{
						Hosts: []*v1.Host{
							{
								Id:           idgen.HostIDV2("172.0.0.1", "foo"),
								Ip:           "172.0.0.1",
								Hostname:     "foo",
								Port:         8003,
								DownloadPort: 8001,
								Location:     "location",
								Idc:          "idc",
							},
						},
					}, nil).Times(1),
					ms.Send(&schedulerv1.SyncProbesRequest{
						Host: mockSeedHost,
						Request: &schedulerv1.SyncProbesRequest_ProbeFailedRequest{
							ProbeFailedRequest: &schedulerv1.ProbeFailedRequest{
								Probes: []*schedulerv1.FailedProbe{
									{
										Host: &v1.Host{
											Id:           idgen.HostIDV2("172.0.0.1", "foo"),
											Ip:           "172.0.0.1",
											Hostname:     "foo",
											Port:         8003,
											DownloadPort: 8001,
											Location:     "location",
											Idc:          "idc",
										},
										Description: "receive packet failed",
									},
								},
							},
						},
					}).Return(errors.New("send ProbeFailedRequest error")).Times(1),
				)
			},
			expect: func(t *testing.T, p Probe, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				go p.(*probe).uploadProbesToScheduler()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			schedulerClient := schedulerclientmocks.NewMockV1(ctl)
			stream := schedulerv1mocks.NewMockScheduler_SyncProbesClient(ctl)
			tc.mock(schedulerClient.EXPECT(), stream, stream.EXPECT())
			mockDaemonConfig.NetworkTopology.Probe.Interval = tc.interval
			probe, err := NewProbe(mockDaemonConfig, mockSeedHost.Id, int32(mockPort), int32(mockDownloadPort), schedulerClient)
			tc.expect(t, probe, err)
			tc.sleep()
			probe.Stop()
		})
	}
}

func TestProbe_collectProbes(t *testing.T) {
	tests := []struct {
		name      string
		destHosts []*v1.Host
		expect    func(t *testing.T, p Probe, err error, destHosts []*v1.Host)
	}{
		{
			name:      "collect probes",
			destHosts: []*v1.Host{mockHost},
			expect: func(t *testing.T, p Probe, err error, destHosts []*v1.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probes, failProbes := p.(*probe).collectProbes(destHosts)
				assert.Equal(len(probes), 1)
				assert.Equal(len(failProbes), 0)
			},
		},
		{
			name: "collect fail probes",
			destHosts: []*v1.Host{
				{
					Id:           idgen.HostIDV2("172.0.0.1", "foo"),
					Ip:           "172.0.0.1",
					Hostname:     "foo",
					Port:         8003,
					DownloadPort: 8001,
					Location:     "location",
					Idc:          "idc",
				},
			},
			expect: func(t *testing.T, p Probe, err error, destHosts []*v1.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probes, failProbes := p.(*probe).collectProbes(destHosts)
				assert.Equal(len(probes), 0)
				assert.Equal(len(failProbes), 1)
			},
		},
		{
			name: "collect probes and fail probes",
			destHosts: []*v1.Host{
				mockHost,
				{
					Id:           idgen.HostIDV2("172.0.0.1", "foo"),
					Ip:           "172.0.0.1",
					Hostname:     "foo",
					Port:         8003,
					DownloadPort: 8001,
					Location:     "location",
					Idc:          "idc",
				},
			},
			expect: func(t *testing.T, p Probe, err error, destHosts []*v1.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probes, failProbes := p.(*probe).collectProbes(destHosts)
				assert.Equal(len(probes), 1)
				assert.Equal(len(failProbes), 1)
			},
		},
		{
			name:      "dest hosts is empty",
			destHosts: []*v1.Host{},
			expect: func(t *testing.T, p Probe, err error, destHosts []*v1.Host) {
				assert := assert.New(t)
				assert.NoError(err)
				probes, failProbes := p.(*probe).collectProbes(destHosts)
				assert.Equal(len(probes), 0)
				assert.Equal(len(failProbes), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()

			schedulerClient := schedulerclientmocks.NewMockV1(ctl)
			probe, err := NewProbe(mockDaemonConfig, mockSeedHost.Id, int32(mockPort), int32(mockDownloadPort), schedulerClient)
			tc.expect(t, probe, err, tc.destHosts)
		})
	}
}
