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

package transport

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly.v2/client/daemon/test"
	mock_peer "d7y.io/dragonfly.v2/client/daemon/test/mock/peer"
	"d7y.io/dragonfly.v2/pkg/rpc/scheduler"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestTransport_RoundTrip(t *testing.T) {
	assert := testifyassert.New(t)
	ctrl := gomock.NewController(t)
	testData, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	var url = "http://x/y"
	peerTaskManager := mock_peer.NewMockTaskManager(ctrl)
	peerTaskManager.EXPECT().StartStreamPeerTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *scheduler.PeerTaskRequest) (io.ReadCloser, map[string]string, error) {
			assert.Equal(req.Url, url)
			return ioutil.NopCloser(bytes.NewBuffer(testData)), nil, nil
		},
	)
	rt, _ := New(
		WithPeerHost(&scheduler.PeerHost{}),
		WithPeerTaskManager(peerTaskManager),
		WithCondition(func(r *http.Request) bool {
			return true
		}))
	assert.NotNil(rt)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := rt.RoundTrip(req)
	assert.Nil(err)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	output, err := ioutil.ReadAll(resp.Body)
	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal(testData, output)
}

func TestTransport_headerToMap(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		expect func(t *testing.T, data interface{})
	}{
		{
			name: "normal conversion",
			header: http.Header{
				"foo": {"foo"},
				"bar": {"bar"},
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, map[string]string{
					"foo": "foo",
					"bar": "bar",
				})
			},
		},
		{
			name:   "header is empty",
			header: http.Header{},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, map[string]string{})
			},
		},
		{
			name: "header is a nested array",
			header: http.Header{
				"foo": {"foo1", "foo2"},
				"bar": {"bar"},
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, map[string]string{
					"foo": "foo1",
					"bar": "bar",
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := headerToMap(tc.header)
			tc.expect(t, data)
		})
	}
}

func TestTransport_mapToHeader(t *testing.T) {
	tests := []struct {
		name   string
		m      map[string]string
		expect func(t *testing.T, data interface{})
	}{
		{
			name: "normal conversion",
			m: map[string]string{
				"Foo": "foo",
				"Bar": "bar",
			},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, http.Header{
					"Foo": {"foo"},
					"Bar": {"bar"},
				})
			},
		},
		{
			name: "map is empty",
			m:    map[string]string{},
			expect: func(t *testing.T, data interface{}) {
				assert := testifyassert.New(t)
				assert.EqualValues(data, http.Header{})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := mapToHeader(tc.m)
			tc.expect(t, data)
		})
	}
}

func TestTransport_pickHeader(t *testing.T) {
	tests := []struct {
		name         string
		header       http.Header
		key          string
		defaultValue string
		expect       func(t *testing.T, data string, header http.Header)
	}{
		{
			name: "Pick the existing key",
			header: http.Header{
				"Foo": {"foo"},
				"Bar": {"bar"},
			},
			key:          "Foo",
			defaultValue: "",
			expect: func(t *testing.T, data string, header http.Header) {
				assert := testifyassert.New(t)
				assert.Equal("foo", data)
				assert.Equal("", header.Get("Foo"))
			},
		},
		{
			name:         "Pick the non-existent key",
			header:       http.Header{},
			key:          "Foo",
			defaultValue: "bar",
			expect: func(t *testing.T, data string, header http.Header) {
				assert := testifyassert.New(t)
				assert.Equal(data, "bar")
				assert.Equal(header.Get("Foo"), "")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := pickHeader(tc.header, tc.key, tc.defaultValue)
			tc.expect(t, data, tc.header)
		})
	}
}
