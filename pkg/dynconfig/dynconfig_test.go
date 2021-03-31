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

package dynconfig

import (
	"testing"
	"time"

	mock_manager_client "d7y.io/dragonfly/v2/pkg/dynconfig/mocks"
	"github.com/golang/mock/gomock"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
)

type TestDynConfig struct {
	Scheduler SchedulerOption
}

type SchedulerOption struct {
	Name string
}

func TestDynconfigGet_ManagerSourceType(t *testing.T) {
	schedulerName := "scheduler"
	tests := []struct {
		name      string
		expire    time.Duration
		dynconfig TestDynConfig
		mock      func(m *mock_manager_client.MockmanagerClientMockRecorder)
		expect    func(t *testing.T, res interface{})
	}{
		{
			name:   "get dynconfig success",
			expire: 1 * time.Second,
			dynconfig: TestDynConfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynConfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).Times(1)
			},
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynConfig
				mapstructure.Decode(data, &d)
				assert.EqualValues(d, TestDynConfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
		{
			name:   "get expire dynconfig",
			expire: 100 * time.Millisecond,
			dynconfig: TestDynConfig{
				Scheduler: SchedulerOption{
					Name: schedulerName,
				},
			},
			mock: func(m *mock_manager_client.MockmanagerClientMockRecorder) {
				var d map[string]interface{}
				mapstructure.Decode(TestDynConfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				}, &d)
				m.Get().Return(d, nil).AnyTimes()
			},
			expect: func(t *testing.T, data interface{}) {
				assert := assert.New(t)
				var d TestDynConfig
				mapstructure.Decode(data, &d)
				assert.EqualValues(d, TestDynConfig{
					Scheduler: SchedulerOption{
						Name: schedulerName,
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mock_manager_client.NewMockmanagerClient(ctl)
			tc.mock(mockManagerClient.EXPECT())

			d, err := NewDynconfig(ManagerSourceType, tc.expire, []Option{WithManagerClient(mockManagerClient)}...)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(300 * time.Millisecond)
			data, err := d.Get()
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, data)
		})
	}
}
