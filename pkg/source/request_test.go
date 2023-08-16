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

package source

import (
	"context"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func equalRequestWithOutContext(expected, actual *Request) bool {
	return expected.URL.String() == actual.URL.String() && reflect.DeepEqual(expected.Header, actual.Header)
}

func TestNewRequest(t *testing.T) {
	assert := assert.New(t)
	testURL, err := url.Parse("http://www.dragonfly.io")
	assert.Nil(err)
	got, err := NewRequest("http://www.dragonfly.io")
	assert.Nil(err)
	// test NewRequest
	assert.True(equalRequestWithOutContext(&Request{URL: testURL,
		Header: Header{}}, got))
	testHeaderMap := map[string]string{"testKey1": "testValue1", "testKey2": "testValue2"}
	testHeader := Header{"Testkey1": []string{"testValue1"}, "Testkey2": []string{"testValue2"}}
	// test NewRequestWithHeader
	got, err = NewRequestWithHeader("http://www.dragonfly.io", testHeaderMap)
	assert.Nil(err)
	assert.True(equalRequestWithOutContext(&Request{URL: testURL,
		Header: testHeader}, got))

	// test NewRequestWithContext
	testContext, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()
	got, err = NewRequestWithContext(testContext, "http://www.dragonfly.io", testHeaderMap)
	assert.Nil(err)
	contextRequest := &Request{
		URL:    testURL,
		Header: testHeader,
		ctx:    testContext,
	}
	assert.True(equalRequestWithOutContext(contextRequest, got))
	expectedDeadline, _ := testContext.Deadline()
	gotDeadline, _ := got.ctx.Deadline()
	assert.Equal(expectedDeadline, gotDeadline)
}

func TestRequest_Context(t *testing.T) {
	assert := assert.New(t)

	got, err := NewRequest("http://www.dragonfly.io")
	assert.Nil(err)
	assert.Equal(got.Context(), context.Background())

	testHeaderMap := map[string]string{"testKey1": "testValue1", "testKey2": "testValue2"}
	got, err = NewRequestWithHeader("http://www.dragonfly.io", testHeaderMap)
	assert.Nil(err)
	assert.Equal(got.Context(), context.Background())

	testContext, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()
	got, err = NewRequestWithContext(testContext, "http://www.dragonfly.io", testHeaderMap)
	assert.Nil(err)
	assert.Equal(got.Context(), testContext)
}

func TestRequest_WithContext(t *testing.T) {
	var testURL, err = url.Parse("http://www.dragonfly.io")
	testCloneURL, err := url.Parse("http://www.dragonfly.io")

	testHeader := Header{"testKey1": []string{"testValue1"}, "testKey2": []string{"testValue2"}}
	testCloneHeader := Header{"testKey1": []string{"testValue1"}, "testKey2": []string{"testValue2"}}

	testContext := context.Background()

	require := require.New(t)
	require.Nil(err)

	r := &Request{
		URL:    testURL,
		Header: testHeader,
		ctx:    testContext,
	}
	got := r.WithContext(testContext)
	require.EqualValues(&Request{
		URL:    testCloneURL,
		Header: testCloneHeader,
		ctx:    testContext,
	}, got)
}

func TestRequest_Clone(t *testing.T) {
	var testURL, err = url.Parse("http://www.dragonfly.io")
	testCloneURL, err := url.Parse("http://www.dragonfly.io")
	testHeader := Header{"testKey1": []string{"testValue1"}, "testKey2": []string{"testValue2"}}
	testCloneHeader := Header{"testKey1": []string{"testValue1"}, "testKey2": []string{"testValue2"}}
	testContext := context.Background()
	require := require.New(t)
	require.Nil(err)
	r := &Request{
		URL:    testURL,
		Header: testHeader,
		ctx:    testContext,
	}
	got := r.Clone(testContext)
	require.EqualValues(&Request{
		URL:    testCloneURL,
		Header: testCloneHeader,
		ctx:    testContext,
	}, got)
}
