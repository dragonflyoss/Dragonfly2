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

package io

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockReadCloser struct{}

func (m *mockReadCloser) Read(p []byte) (int, error) {
	return 1, nil
}

func (m *mockReadCloser) Close() error {
	return nil
}

type mockReadCloserWithReadError struct{}

func (m *mockReadCloserWithReadError) Read(p []byte) (int, error) {
	return 0, errors.New("foo")
}

func (m *mockReadCloserWithReadError) Close() error {
	return nil
}

type mockReadCloserWithCloseError struct{}

func (m *mockReadCloserWithCloseError) Read(p []byte) (int, error) {
	return 1, nil
}

func (m *mockReadCloserWithCloseError) Close() error {
	return errors.New("foo")
}

func TestMultiReadCloser(t *testing.T) {
	assert := assert.New(t)
	readCloser := MultiReadCloser(&mockReadCloser{})
	n, err := readCloser.Read([]byte{})
	assert.NoError(err)
	assert.Equal(n, 1)

	err = readCloser.Close()
	assert.NoError(err)

	readCloser = MultiReadCloser(&mockReadCloserWithReadError{})
	n, err = readCloser.Read([]byte{})
	assert.Error(err)
	assert.Equal(n, 0)

	err = readCloser.Close()
	assert.NoError(err)

	readCloser = MultiReadCloser(&mockReadCloserWithCloseError{})
	n, err = readCloser.Read([]byte{})
	assert.NoError(err)
	assert.Equal(n, 1)

	err = readCloser.Close()
	assert.Error(err)
}
