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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/cdn/dynconfig"
	dynconfigInternal "d7y.io/dragonfly/v2/internal/dynconfig"
	managerGRPC "d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
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
