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

package config

import (
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func Test_NewDfstore(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, cfg *DfstoreConfig)
	}{
		{
			name: "new dfstore configuration",
			expect: func(t *testing.T, cfg *DfstoreConfig) {
				assert := testifyassert.New(t)
				assert.Equal("http://127.0.0.1:65004", cfg.Endpoint)
				assert.Equal(3, cfg.MaxReplicas)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewDfstore()
			tc.expect(t, cfg)
		})
	}
}

func TestDfstoreConfig_Validate(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *DfstoreConfig
		expect func(t *testing.T, err error)
	}{
		{
			name: "normal dfsrore",
			cfg: &DfstoreConfig{
				Endpoint:    "http://127.0.0.1:65004",
				MaxReplicas: 3,
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.Nil(err)
			},
		},
		{
			name: "dfstore without endpoint",
			cfg: &DfstoreConfig{
				Endpoint:    "",
				MaxReplicas: 3,
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "dfstore requires parameter endpoint")
			},
		},
		{
			name: "dfstore with invalid endpoint",
			cfg: &DfstoreConfig{
				Endpoint:    "127.0.0.1:65004",
				MaxReplicas: 3,
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "invalid endpoint: parse \"127.0.0.1:65004\": invalid URI for request")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			tc.expect(t, err)
		})
	}
}
