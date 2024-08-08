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

package evaluator

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	networktopologymocks "d7y.io/dragonfly/v2/scheduler/networktopology/mocks"
)

func TestEvaluator_New(t *testing.T) {
	pluginDir := "."
	ctl := gomock.NewController(t)
	defer ctl.Finish()
	mockNetworkTopology := networktopologymocks.NewMockNetworkTopology(ctl)
	tests := []struct {
		name      string
		algorithm string
		options   []NetworkTopologyOption
		expect    func(t *testing.T, e any)
	}{
		{
			name:      "new evaluator with default algorithm",
			algorithm: "default",
			options:   []NetworkTopologyOption{},
			expect: func(t *testing.T, e any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(e).Elem().Name(), "evaluatorBase")
			},
		},
		{
			name:      "new evaluator with machine learning algorithm",
			algorithm: "ml",
			options:   []NetworkTopologyOption{},
			expect: func(t *testing.T, e any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(e).Elem().Name(), "evaluatorBase")
			},
		},
		{
			name:      "new evaluator with plugin",
			algorithm: "plugin",
			options:   []NetworkTopologyOption{},
			expect: func(t *testing.T, e any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(e).Elem().Name(), "evaluatorBase")
			},
		},
		{
			name:      "new evaluator with empty string",
			algorithm: "",
			options:   []NetworkTopologyOption{},
			expect: func(t *testing.T, e any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(e).Elem().Name(), "evaluatorBase")
			},
		},
		{
			name:      "new evaluator with default algorithm and networkTopology",
			algorithm: "nt",
			options:   []NetworkTopologyOption{WithNetworkTopology(mockNetworkTopology)},
			expect: func(t *testing.T, e any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(e).Elem().Name(), "evaluatorNetworkTopology")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, New(tc.algorithm, pluginDir, tc.options...))
		})
	}
}
