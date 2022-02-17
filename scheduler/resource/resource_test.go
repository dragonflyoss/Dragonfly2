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

package resource

import (
	"errors"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/gc"
	"d7y.io/dragonfly/v2/scheduler/config"
	configmocks "d7y.io/dragonfly/v2/scheduler/config/mocks"
)

func TestResource_New(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		mock   func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder)
		expect func(t *testing.T, resource Resource, err error)
	}{
		{
			name:   "new resource",
			config: config.New(),
			mock: func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					gc.Add(gomock.Any()).Return(nil).Times(3),
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{{ID: 1}},
					}, nil).Times(1),
					dynconfig.Register(gomock.Any()).Return().Times(1),
				)
			},
			expect: func(t *testing.T, resource Resource, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(resource).Elem().Name(), "resource")
				assert.NoError(err)
			},
		},
		{
			name:   "new resource failed because of host manager error",
			config: config.New(),
			mock: func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					gc.Add(gomock.Any()).Return(errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, resource Resource, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name:   "new resource failed because of task manager error",
			config: config.New(),
			mock: func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					gc.Add(gomock.Any()).Return(nil).Times(1),
					gc.Add(gomock.Any()).Return(errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, resource Resource, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name:   "new resource failed because of peer manager error",
			config: config.New(),
			mock: func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					gc.Add(gomock.Any()).Return(nil).Times(2),
					gc.Add(gomock.Any()).Return(errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, resource Resource, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name:   "new resource faild because of dynconfig get error",
			config: config.New(),
			mock: func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					gc.Add(gomock.Any()).Return(nil).Times(3),
					dynconfig.Get().Return(nil, errors.New("foo")).Times(1),
				)
			},
			expect: func(t *testing.T, resource Resource, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "foo")
			},
		},
		{
			name:   "new resource faild because of cdn list is empty",
			config: config.New(),
			mock: func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					gc.Add(gomock.Any()).Return(nil).Times(3),
					dynconfig.Get().Return(&config.DynconfigData{
						CDNs: []*config.CDN{},
					}, nil).Times(1),
				)
			},
			expect: func(t *testing.T, resource Resource, err error) {
				assert := assert.New(t)
				assert.EqualError(err, "address list of cdn is empty")
			},
		},
		{
			name: "new resource with cdn",
			config: &config.Config{
				Scheduler: &config.SchedulerConfig{
					GC: &config.GCConfig{
						PeerGCInterval: 100,
						PeerTTL:        1000,
						TaskGCInterval: 100,
						TaskTTL:        1000,
						HostGCInterval: 100,
						HostTTL:        1000,
					},
				},
				CDN: &config.CDNConfig{
					Enable: false,
				},
			},
			mock: func(gc *gc.MockGCMockRecorder, dynconfig *configmocks.MockDynconfigInterfaceMockRecorder) {
				gomock.InOrder(
					gc.Add(gomock.Any()).Return(nil).Times(3),
				)
			},
			expect: func(t *testing.T, resource Resource, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(resource).Elem().Name(), "resource")
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			gc := gc.NewMockGC(ctl)
			dynconfig := configmocks.NewMockDynconfigInterface(ctl)
			tc.mock(gc.EXPECT(), dynconfig.EXPECT())

			resource, err := New(tc.config, gc, dynconfig)
			tc.expect(t, resource, err)
		})
	}
}
