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

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
)

func TestTaskIDV1(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		meta        *commonv1.UrlMeta
		ignoreRange bool
		expect      func(t *testing.T, d any)
	}{
		{
			name: "generate taskID with url",
			url:  "https://example.com",
			meta: nil,
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9")
			},
		},
		{
			name: "generate taskID with meta",
			url:  "https://example.com",
			meta: &commonv1.UrlMeta{
				Range:  "foo",
				Digest: "bar",
				Tag:    "",
			},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "aeee0e0a2a0c75130582641353c539aaf9011a0088b31347f7588e70e449a3e0")
			},
		},
		{
			name: "generate taskID with meta",
			url:  "https://example.com",
			meta: &commonv1.UrlMeta{
				Range:  "foo",
				Digest: "bar",
				Tag:    "",
			},
			ignoreRange: true,
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "63dee2822037636b0109876b58e95692233840753a882afa69b9b5ee82a6c57d")
			},
		},
		{
			name: "generate taskID with filter",
			url:  "https://example.com?foo=foo&bar=bar",
			meta: &commonv1.UrlMeta{
				Tag:    "foo",
				Filter: "foo&bar",
			},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b")
			},
		},
		{
			name: "generate taskID with tag",
			url:  "https://example.com",
			meta: &commonv1.UrlMeta{
				Tag: "foo",
			},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var data string
			if tc.ignoreRange {
				data = ParentTaskIDV1(tc.url, tc.meta)
			} else {
				data = TaskIDV1(tc.url, tc.meta)
			}
			tc.expect(t, data)
		})
	}
}

func TestTaskIDV2(t *testing.T) {
	tests := []struct {
		name        string
		url         string
		tag         string
		application string
		filters     []string
		expect      func(t *testing.T, d any)
	}{
		{
			name:        "generate taskID",
			url:         "https://example.com",
			tag:         "foo",
			application: "bar",
			filters:     []string{},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "160fa7f001d9d2e893130894fbb60a5fb006e1d61bff82955f2946582bc9de1d")
			},
		},
		{
			name: "generate taskID with tag",
			url:  "https://example.com",
			tag:  "foo",
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b")
			},
		},
		{
			name:        "generate taskID with application",
			url:         "https://example.com",
			application: "bar",
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "63dee2822037636b0109876b58e95692233840753a882afa69b9b5ee82a6c57d")
			},
		},
		{
			name:    "generate taskID with filters",
			url:     "https://example.com?foo=foo&bar=bar",
			filters: []string{"foo", "bar"},
			expect: func(t *testing.T, d any) {
				assert := assert.New(t)
				assert.Equal(d, "100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, TaskIDV2(tc.url, tc.tag, tc.application, tc.filters))
		})
	}
}
