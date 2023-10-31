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

package rpcserver

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"d7y.io/dragonfly/v2/trainer/config"
	storagemocks "d7y.io/dragonfly/v2/trainer/storage/mocks"
	trainingmocks "d7y.io/dragonfly/v2/trainer/training/mocks"
)

func TestRPCServer_New(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s any)
	}{
		{
			name: "new server",
			expect: func(t *testing.T, s any) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "Server")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			storage := storagemocks.NewMockStorage(ctl)
			training := trainingmocks.NewMockTraining(ctl)

			svr := New(&config.Config{}, storage, training)
			tc.expect(t, svr)
		})
	}
}
