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

package rpc

import (
	"bytes"
	"io"
	"net"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

type testConn struct {
	net.Conn
	buf *bytes.Buffer
}

func (conn *testConn) Read(b []byte) (int, error) {
	return conn.buf.Read(b)
}

func Test_muxConn(t *testing.T) {
	assert := testifyassert.New(t)

	testCases := []struct {
		name    string
		bufSize int
		data    []byte
	}{
		{
			name:    "buf size equal data size",
			bufSize: 4,
			data:    []byte("hell"),
		},
		{
			name:    "buf size less than data size",
			bufSize: 4,
			data:    []byte("hello world"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mc := &muxConn{
				buf:  tc.data[:tc.bufSize],
				Conn: &testConn{Conn: nil, buf: bytes.NewBuffer(tc.data[tc.bufSize:])},
			}

			data, err := io.ReadAll(mc)
			assert.Nil(err, "read all should ok")
			assert.Equal(tc.data, data, "data shloud be same")
		})
	}
}
