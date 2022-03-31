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

package digestutils

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func TestNewDigestReader(t *testing.T) {
	testBytes := []byte("hello world")
	tests := []struct {
		name   string
		digest string
		expect func(t *testing.T, reader io.Reader, err error)
	}{
		{
			name:   "md5",
			digest: fmt.Sprintf("md5:%s", Md5Bytes(testBytes)),
			expect: func(t *testing.T, reader io.Reader, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				data, err := io.ReadAll(reader)
				assert.Nil(err)
				assert.Equal(testBytes, data)
			},
		},
		{
			name:   "sha256",
			digest: fmt.Sprintf("sha256:%s", Sha256Bytes(testBytes)),
			expect: func(t *testing.T, reader io.Reader, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				data, err := io.ReadAll(reader)
				assert.Nil(err)
				assert.Equal(testBytes, data)
			},
		},
		{
			name:   "md5 and not specified hex algorithm",
			digest: Md5Bytes(testBytes),
			expect: func(t *testing.T, reader io.Reader, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				data, err := io.ReadAll(reader)
				assert.Nil(err)
				assert.Equal(testBytes, data)
			},
		},
		{
			name:   "sha256 and not specified hex algorithm should return error",
			digest: Sha256Bytes(testBytes),
			expect: func(t *testing.T, reader io.Reader, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				_, err = io.ReadAll(reader)
				assert.NotNil(err)
			},
		},
		{
			name:   "not specified hex algorithm and use default md5",
			digest: "",
			expect: func(t *testing.T, reader io.Reader, err error) {
				assert := assert.New(t)
				assert.Nil(err)
				data, err := io.ReadAll(reader)
				assert.Nil(err)
				assert.Equal(testBytes, data)
			},
		},
		{
			name:   "not support sh1 hex algorithm",
			digest: "sha1:xxx",
			expect: func(t *testing.T, reader io.Reader, err error) {
				assert := assert.New(t)
				assert.Nil(reader)
				assert.EqualError(err, "digest algorithm sha1 is not support")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewBuffer(testBytes)
			reader, err := NewDigestReader(logger.With("test", "test"), buf, tc.digest)
			tc.expect(t, reader, err)
		})
	}
}
