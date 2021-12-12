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

package dfpath

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
		expect  func(t *testing.T, options []Option)
	}{
		{
			name: "new dfpath succeeded",
			expect: func(t *testing.T, options []Option) {
				assert := assert.New(t)
				d, err := New(options...)
				assert.NoError(err)
				assert.Equal(d.WorkHome(), DefaultWorkHome)
				assert.Equal(d.CacheDir(), DefaultCacheDir)
				assert.Equal(d.LogDir(), DefaultLogDir)
				assert.Equal(d.DataDir(), DefaultDataDir)
			},
		},
		{
			name:    "new dfpath succeeded by workHome",
			options: []Option{WithWorkHome("foo")},
			expect: func(t *testing.T, options []Option) {
				assert := assert.New(t)
				d, err := New(options...)
				assert.NoError(err)
				assert.Equal(d.WorkHome(), DefaultWorkHome)
				assert.Equal(d.CacheDir(), DefaultCacheDir)
				assert.Equal(d.LogDir(), DefaultLogDir)
				assert.Equal(d.DataDir(), DefaultDataDir)
			},
		},
		{
			name:    "new dfpath succeeded by cacheDir",
			options: []Option{WithCacheDir("foo")},
			expect: func(t *testing.T, options []Option) {
				assert := assert.New(t)
				d, err := New(options...)
				assert.NoError(err)
				assert.Equal(d.WorkHome(), DefaultWorkHome)
				assert.Equal(d.CacheDir(), DefaultCacheDir)
				assert.Equal(d.LogDir(), DefaultLogDir)
				assert.Equal(d.DataDir(), DefaultDataDir)
			},
		},
		{
			name:    "new dfpath succeeded by logDir",
			options: []Option{WithLogDir("foo")},
			expect: func(t *testing.T, options []Option) {
				assert := assert.New(t)
				d, err := New(options...)
				assert.NoError(err)
				assert.Equal(d.WorkHome(), DefaultWorkHome)
				assert.Equal(d.CacheDir(), DefaultCacheDir)
				assert.Equal(d.LogDir(), DefaultLogDir)
				assert.Equal(d.DataDir(), DefaultDataDir)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, tc.options)
		})
	}
}
