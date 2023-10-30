/*
 *     Copyright 2022 The Dragonfly Authors
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

package announcer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/client/config"
	configmocks "d7y.io/dragonfly/v2/client/config/mocks"
	managerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	schedulerclientmocks "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client/mocks"
)

func TestAnnouncer_New(t *testing.T) {
	tests := []struct {
		name               string
		config             *config.DaemonOption
		hostID             string
		deamonPort         int32
		deamonDownloadPort int32
		expect             func(t *testing.T, announcer Announcer)
	}{
		{
			name:               "new announcer",
			config:             &config.DaemonOption{},
			hostID:             "foo",
			deamonPort:         8000,
			deamonDownloadPort: 8001,
			expect: func(t *testing.T, a Announcer) {
				assert := assert.New(t)
				instance := a.(*announcer)
				assert.NotNil(instance.config)
				assert.Equal(instance.hostID, "foo")
				assert.Equal(instance.daemonPort, int32(8000))
				assert.Equal(instance.daemonDownloadPort, int32(8001))
				assert.NotNil(instance.managerClient)
				assert.NotNil(instance.schedulerClient)
			},
		},
		{
			name:               "new announcer with empty value",
			config:             nil,
			hostID:             "",
			deamonPort:         0,
			deamonDownloadPort: 0,
			expect: func(t *testing.T, a Announcer) {
				assert := assert.New(t)
				instance := a.(*announcer)
				assert.Nil(instance.config)
				assert.Equal(instance.hostID, "")
				assert.Equal(instance.daemonPort, int32(0))
				assert.Equal(instance.daemonDownloadPort, int32(0))
				assert.NotNil(instance.managerClient)
				assert.NotNil(instance.schedulerClient)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := managerclientmocks.NewMockV1(ctl)
			mockSchedulerClient := schedulerclientmocks.NewMockV1(ctl)
			mockDynconfig := configmocks.NewMockDynconfig(ctl)
			tc.expect(t, New(tc.config, mockDynconfig, tc.hostID, tc.deamonPort, tc.deamonDownloadPort, mockSchedulerClient, WithManagerClient(mockManagerClient)))
		})
	}
}
