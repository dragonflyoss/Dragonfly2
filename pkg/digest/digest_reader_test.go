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

package digest

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"io"
	"os"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestNewReader(t *testing.T) {
	assert := testifyassert.New(t)

	testCases := []struct {
		name   string
		data   []byte
		digest func(data []byte) string
	}{
		{
			name: "default md5",
			data: []byte("hello world"),
			digest: func(data []byte) string {
				hash := md5.New()
				hash.Write(data)
				return hex.EncodeToString(hash.Sum(nil))
			},
		},
		{
			name: "md5",
			data: []byte("hello world"),
			digest: func(data []byte) string {
				hash := md5.New()
				hash.Write(data)
				return "md5:" + hex.EncodeToString(hash.Sum(nil))
			},
		},
		{
			name: "sha1",
			data: []byte("hello world"),
			digest: func(data []byte) string {
				hash := sha1.New()
				hash.Write(data)
				return "sha1:" + hex.EncodeToString(hash.Sum(nil))
			},
		},
		{
			name: "sha256",
			data: []byte("hello world"),
			digest: func(data []byte) string {
				hash := sha256.New()
				hash.Write(data)
				return "sha256:" + hex.EncodeToString(hash.Sum(nil))
			},
		},
		{
			name: "sha512",
			data: []byte("hello world"),
			digest: func(data []byte) string {
				hash := sha512.New()
				hash.Write(data)
				return "sha512:" + hex.EncodeToString(hash.Sum(nil))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			digest := tc.digest(tc.data)
			buf := bytes.NewBuffer(tc.data)
			reader, err := NewReader(logger.With("test", "test"), buf, digest)
			assert.Nil(err)
			data, err := io.ReadAll(reader)
			assert.Nil(err)
			assert.Equal(tc.data, data)
		})
	}
}
