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

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/rpc/base"
)

func TestTaskID(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		meta        *base.UrlMeta
		ignoreRange bool
		expect      func(t *testing.T, d any)
	}{
		{
			name: "generate taskID with url",
			url:  "https://example.com",
			meta: nil,
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal("100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9", d)
			},
		},
		{
			name: "generate taskID with meta",
			url:  "https://example.com",
			meta: &base.UrlMeta{
				Range:  "foo",
				Digest: "bar",
				Tag:    "",
			},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal("aeee0e0a2a0c75130582641353c539aaf9011a0088b31347f7588e70e449a3e0", d)
			},
		},
		{
			name: "generate taskID with meta",
			url:  "https://example.com",
			meta: &base.UrlMeta{
				Range:  "foo",
				Digest: "bar",
				Tag:    "",
			},
			ignoreRange: true,
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal("63dee2822037636b0109876b58e95692233840753a882afa69b9b5ee82a6c57d", d)
			},
		},
		{
			name: "generate taskID with filter",
			url:  "https://example.com?foo=foo&bar=bar",
			meta: &base.UrlMeta{
				Tag:    "foo",
				Filter: "foo&bar",
			},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal("2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b", d)
			},
		},
		{
			name: "generate taskID with tag",
			url:  "https://example.com",
			meta: &base.UrlMeta{
				Tag: "foo",
			},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal("2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b", d)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var data string
			if tc.ignoreRange {
				data = ParentTaskID(tc.url, tc.meta)
			} else {
				data = TaskID(tc.url, tc.meta)
			}
			tc.expect(t, data)
		})
	}
}
