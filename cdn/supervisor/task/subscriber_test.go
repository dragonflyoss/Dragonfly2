/*
 *     Copyright 2020 The Dragonfly Authors
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

package task

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/cdn/dynconfig"
	dynconfigMocks "d7y.io/dragonfly/v2/cdn/dynconfig/mocks"
	dynconfigInternal "d7y.io/dragonfly/v2/internal/dynconfig"
	managerGRPC "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestNotifySchedulerGCSubscriber_OnNotify(t *testing.T) {
	cacheDir := t.TempDir()
	defer assert.Nil(t, os.RemoveAll(cacheDir))
	ctl := gomock.NewController(t)
	mockManagerClient := mocks.NewMockClient(ctl)
	beforeSchedulers := []*managerGRPC.Scheduler{
		{
			HostName: "host1",
			Ip:       "127.0.0.1",
			Port:     9000,
		},
		{
			HostName: "host2",
			Ip:       "127.0.0.2",
			Port:     9000,
		},
	}
	afterSchedulers := []*managerGRPC.Scheduler{
		{
			HostName: "host3",
			Ip:       "127.0.0.3",
			Port:     9001,
		},
		{
			HostName: "host4",
			Ip:       "127.0.0.4",
			Port:     9001,
		},
	}
	mockManagerClient.EXPECT().GetCDN(gomock.Any()).Return(&managerGRPC.CDN{
		Schedulers: beforeSchedulers,
	}, nil).Times(1)
	d, err := dynconfig.NewDynconfig(dynconfig.Config{
		RefreshInterval: 10 * time.Millisecond,
		CachePath:       filepath.Join(cacheDir, "cdn"),
		SourceType:      dynconfigInternal.ManagerSourceType,
	}, func() (interface{}, error) {
		cdn, err := mockManagerClient.GetCDN(&managerGRPC.GetCDNRequest{
			HostName:     "cdn",
			SourceType:   managerGRPC.SourceType_SCHEDULER_SOURCE,
			CdnClusterId: 10,
		})
		if err != nil {
			return nil, err
		}
		return cdn, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	gcSubscriber, err := NewNotifySchedulerTaskGCSubscriber(d)
	assert.Nil(t, err)
	schedulerMap := gcSubscriber.GetSchedulers()
	testEqual(t, schedulerMap, beforeSchedulers)
	mockManagerClient.EXPECT().GetCDN(gomock.Any()).Return(&managerGRPC.CDN{
		Schedulers: afterSchedulers,
	}, nil).Times(1)
	time.Sleep(15 * time.Millisecond)
	schedulerMap = gcSubscriber.GetSchedulers()
	testEqual(t, schedulerMap, afterSchedulers)
}

func testEqual(t *testing.T, actualMap map[string]string, schedulers []*managerGRPC.Scheduler) {
	assert.Equal(t, len(schedulers), len(actualMap))
	var expectedMap = make(map[string]string, len(schedulers))
	for _, scheduler := range schedulers {
		expectedMap[scheduler.Ip] = fmt.Sprintf("%s:%d", scheduler.Ip, scheduler.Port)
	}
	for key, value := range actualMap {
		assert.Equal(t, expectedMap[key], value)
	}
}

func TestNotifySchedulerGCSubscriber_GC(t *testing.T) {
	ctl := gomock.NewController(t)
	d := dynconfigMocks.NewMockInterface(ctl)
	testServerData, err := startTestServers(2)
	assert.Nil(t, err)
	d.EXPECT().Register(gomock.Any()).DoAndReturn(
		func(observer dynconfig.Observer) {
			observer.OnNotify(&managerGRPC.CDN{
				Schedulers: []*managerGRPC.Scheduler{
					{
						HostName: "host1",
						Ip:       testServerData.addressesIP[0],
						Port:     testServerData.addressesPort[0],
					},
					{
						HostName: "host2",
						Ip:       testServerData.addressesIP[1],
						Port:     testServerData.addressesPort[1],
					},
				},
			})
		})
	gcSubscriberInterface, err := NewNotifySchedulerTaskGCSubscriber(d)
	assert.Nil(t, err)
	gcSubscriber := gcSubscriberInterface.(*notifySchedulerGCSubscriber)
	gcSubscriber.AddGCSubscriberInstance("task1", &GCSubscriberInstance{
		ClientIP: testServerData.addressesIP[0],
		PeerID:   "peer1",
	})
	gcSubscriber.AddGCSubscriberInstance("task1", &GCSubscriberInstance{
		ClientIP: testServerData.addressesIP[1],
		PeerID:   "peer2",
	})
	gcSubscriber.AddGCSubscriberInstance("task2", &GCSubscriberInstance{
		ClientIP: testServerData.addressesIP[0],
		PeerID:   "peer2",
	})
	assert.Equal(t, 2, len(gcSubscriber.taskSubscribers))
	gcSubscriber.GC("task1")
	assert.Equal(t, 1, len(gcSubscriber.taskSubscribers))
	gcSubscriber.GC("task2")
	assert.Equal(t, 0, len(gcSubscriber.taskSubscribers))
}

type testServer struct {
	scheduler.UnimplementedSchedulerServer
}

func (s *testServer) LeaveTask(context.Context, *scheduler.PeerTarget) (*emptypb.Empty, error) {
	return new(emptypb.Empty), nil
}

type testServerData struct {
	servers       []*grpc.Server
	serverImpls   []*testServer
	addressesIP   []string
	addressesPort []int32
}

func (t *testServerData) cleanup() {
	for _, s := range t.servers {
		s.Stop()
	}
}

func startTestServers(count int) (_ *testServerData, err error) {
	t := &testServerData{}
	defer func() {
		if err != nil {
			t.cleanup()
		}
	}()
	for i := 0; i < count; i++ {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			return nil, fmt.Errorf("failed to listen %v", err)
		}

		s := grpc.NewServer()
		sImpl := new(testServer)
		scheduler.RegisterSchedulerServer(s, sImpl)
		t.servers = append(t.servers, s)
		t.serverImpls = append(t.serverImpls, sImpl)
		host, port, err := net.SplitHostPort(lis.Addr().String())
		if err != nil {
			return nil, fmt.Errorf("failed to split lis addr %v", err)
		}
		t.addressesIP = append(t.addressesIP, host)
		portInt, _ := strconv.Atoi(port)
		t.addressesPort = append(t.addressesPort, int32(portInt))

		go func(s *grpc.Server, l net.Listener) {
			if err := s.Serve(l); err != nil {
				log.Fatalf("failed to serve %v", err)
			}
		}(s, lis)
	}

	return t, nil
}
