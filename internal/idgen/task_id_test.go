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

package idgen

import (
	"testing"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"github.com/stretchr/testify/assert"
)

func TestTaskID(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		filter string
		meta   *base.UrlMeta
		bizID  string
		expect func(t *testing.T, d interface{})
	}{
		{
			name:   "generate taskID with url",
			url:    "https://example.com",
			filter: "",
			meta:   nil,
			bizID:  "",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9", d)
			},
		},
		{
			name:   "generate taskID with meta",
			url:    "https://example.com",
			filter: "",
			meta: &base.UrlMeta{
				Range:  "foo",
				Digest: "bar",
			},
			bizID: "",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("aeee0e0a2a0c75130582641353c539aaf9011a0088b31347f7588e70e449a3e0", d)
			},
		},
		{
			name:   "generate taskID with filter",
			url:    "https://example.com?foo=foo&bar=bar",
			filter: "foo&bar",
			meta:   nil,
			bizID:  "",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9", d)
			},
		},
		{
			name:   "generate taskID with bizID",
			url:    "https://example.com",
			filter: "",
			meta:   nil,
			bizID:  "foo",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b", d)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := TaskID(tc.url, tc.filter, tc.meta, tc.bizID)
			tc.expect(t, data)
		})
	}
}

func TestTwinsTaskID(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		filter string
		meta   *base.UrlMeta
		bizID  string
		peerID string
		expect func(t *testing.T, d interface{})
	}{
		{
			name:   "generate taskID with url",
			url:    "https://example.com",
			filter: "",
			meta:   nil,
			bizID:  "",
			peerID: "foo",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9_B", d)
			},
		},
		{
			name:   "generate taskID with meta",
			url:    "https://example.com",
			filter: "",
			meta: &base.UrlMeta{
				Range:  "foo",
				Digest: "bar",
			},
			bizID:  "",
			peerID: "foo",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("aeee0e0a2a0c75130582641353c539aaf9011a0088b31347f7588e70e449a3e0_B", d)
			},
		},
		{
			name:   "generate taskID with filter",
			url:    "https://example.com?foo=foo&bar=bar",
			filter: "foo&bar",
			meta:   nil,
			bizID:  "",
			peerID: "foo",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9_B", d)
			},
		},
		{
			name:   "generate taskID with bizID",
			url:    "https://example.com",
			filter: "",
			meta:   nil,
			bizID:  "foo",
			peerID: "foo",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b_B", d)
			},
		},
		{
			name:   "generate twinsA taskID",
			url:    "https://example.com",
			filter: "",
			meta:   nil,
			bizID:  "",
			peerID: "bar",
			expect: func(t *testing.T, d interface{}) {
				assert := assert.New(t)
				assert.Equal("100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9_A", d)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := TwinsTaskID(tc.url, tc.filter, tc.meta, tc.bizID, tc.peerID)
			tc.expect(t, data)
		})
	}
}
