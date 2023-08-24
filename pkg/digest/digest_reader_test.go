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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func TestDigest_Reader(t *testing.T) {
	log := logger.With("test", "digest")

	tests := []struct {
		name      string
		algorithm string
		data      []byte
		options   []Option
		run       func(t *testing.T, data []byte, reader Reader, err error)
	}{
		{
			name:      "sha1 reader",
			algorithm: AlgorithmSHA1,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log)},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "da39a3ee5e6b4b0d3255bfef95601890afd80709")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33")
			},
		},
		{
			name:      "sha256 reader",
			algorithm: AlgorithmSHA256,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log)},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
			},
		},
		{
			name:      "sha512 reader",
			algorithm: AlgorithmSHA512,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log)},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda19594a7eb539453e1ed7")
			},
		},
		{
			name:      "md5 reader",
			algorithm: AlgorithmMD5,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log)},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "d41d8cd98f00b204e9800998ecf8427e")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "acbd18db4cc2f85cedef654fccc4a4d8")
			},
		},
		{
			name:      "sha1 reader with encoded",
			algorithm: AlgorithmSHA1,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "da39a3ee5e6b4b0d3255bfef95601890afd80709")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33")
			},
		},
		{
			name:      "sha256 reader with encoded",
			algorithm: AlgorithmSHA256,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
			},
		},
		{
			name:      "sha512 reader with encoded",
			algorithm: AlgorithmSHA512,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda19594a7eb539453e1ed7")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "f7fbba6e0636f890e56fbbf3283e524c6fa3204ae298382d624741d0dc6638326e282c41be5e4254d8820772c5518a2c5a8c0c7f7eda19594a7eb539453e1ed7")
			},
		},
		{
			name:      "md5 reader with encoded",
			algorithm: AlgorithmMD5,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("acbd18db4cc2f85cedef654fccc4a4d8")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "d41d8cd98f00b204e9800998ecf8427e")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "acbd18db4cc2f85cedef654fccc4a4d8")
			},
		},
		{
			name:      "sha1 reader with invalid encoded",
			algorithm: AlgorithmSHA1,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("bar")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "da39a3ee5e6b4b0d3255bfef95601890afd80709")
				_, err = io.ReadAll(reader)
				assert.Error(err)
			},
		},
		{
			name:      "sha256 reader with invalid encoded",
			algorithm: AlgorithmSHA256,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("bar")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
				_, err = io.ReadAll(reader)
				assert.Error(err)
			},
		},
		{
			name:      "sha512 reader with invalid encoded",
			algorithm: AlgorithmSHA512,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("bar")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e")
				_, err = io.ReadAll(reader)
				assert.Error(err)
			},
		},
		{
			name:      "md5 reader with invalid encoded",
			algorithm: AlgorithmMD5,
			data:      []byte("foo"),
			options:   []Option{WithLogger(log), WithEncoded("bar")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "d41d8cd98f00b204e9800998ecf8427e")
				_, err = io.ReadAll(reader)
				assert.Error(err)
			},
		},
		{
			name:      "new reader with invalid algorithm",
			algorithm: "",
			data:      []byte("foo"),
			options:   []Option{WithLogger(log)},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name:      "sha1 reader without logger",
			algorithm: AlgorithmSHA1,
			data:      []byte("foo"),
			options:   []Option{WithEncoded("")},
			run: func(t *testing.T, data []byte, reader Reader, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reader.Encoded(), "da39a3ee5e6b4b0d3255bfef95601890afd80709")
				b, err := io.ReadAll(reader)
				assert.NoError(err)
				assert.Equal(b, data)
				assert.Equal(reader.Encoded(), "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(tc.algorithm, bytes.NewBuffer(tc.data), tc.options...)
			tc.run(t, tc.data, r, err)
		})
	}
}
