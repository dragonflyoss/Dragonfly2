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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
)

func TestAnnouncer_New(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		mock   func(m *mocks.MockClientMockRecorder)
		expect func(t *testing.T, announcer Announcer, err error)
	}{
		{
			name: "new announcer",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: "127.0.0.1",
					Port:        8080,
				},
				Host: config.HostConfig{
					IDC:         "foo",
					Location:    "bar",
					NetTopology: "baz",
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				instance := a.(*announcer)
				assert.NoError(err)
				assert.NotNil(instance.config)
				assert.NotNil(instance.managerClient)
			},
		},
		{
			name: "update scheduler failed",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: "127.0.0.1",
					Port:        8080,
				},
				Host: config.HostConfig{
					IDC:         "foo",
					Location:    "bar",
					NetTopology: "baz",
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			mock: func(m *mocks.MockClientMockRecorder) {
				m.UpdateScheduler(gomock.Any(), gomock.Any()).Return(nil, errors.New("foo")).Times(1)
			},
			expect: func(t *testing.T, a Announcer, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			a, err := New(tc.config, mockManagerClient)
			tc.expect(t, a, err)
		})
	}
}
