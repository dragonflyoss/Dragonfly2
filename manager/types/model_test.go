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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MakeObjectKeyOfModelFile(t *testing.T) {
	tests := []struct {
		name      string
		modelName string
		version   int
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make objectKey of model file",
			modelName: "foo",
			version:   1,
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "foo/1/model.graphdef")
			},
		},
		{
			name:      "modelName is empty",
			modelName: "",
			version:   1,
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "/1/model.graphdef")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeObjectKeyOfModelFile(tc.modelName, tc.version))
		})
	}
}

func Test_MakeObjectKeyOfModelConfigFile(t *testing.T) {
	tests := []struct {
		name      string
		modelName string
		version   int
		expect    func(t *testing.T, s string)
	}{
		{
			name:      "make objectKey of model file",
			modelName: "foo",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "foo/config.pbtxt")
			},
		},
		{
			name:      "modelName is empty",
			modelName: "",
			expect: func(t *testing.T, s string) {
				assert := assert.New(t)
				assert.Equal(s, "/config.pbtxt")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, MakeObjectKeyOfModelConfigFile(tc.modelName))
		})
	}
}
