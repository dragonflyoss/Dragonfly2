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
	"io"

	"github.com/hashicorp/go-multierror"
)

// multiReadCloser is MultiReadCloser instance.
type multiReadCloser struct {
	reader  io.Reader
	closers []io.Closer
}

// MultiReadCloser returns a ReadCloser that's the logical concatenation of
// the provided input readClosers.
func MultiReadCloser(readClosers ...io.ReadCloser) io.ReadCloser {
	readers := make([]io.Reader, len(readClosers))
	closers := make([]io.Closer, len(readClosers))
	for readCloser := range readClosers {
		closers[readCloser] = readClosers[readCloser]
		readers[readCloser] = readClosers[readCloser]
	}

	return &multiReadCloser{
		reader:  io.MultiReader(readers...),
		closers: closers,
	}
}

// Reader is the interface that wraps the basic Read method for multiReadCloser.
func (m *multiReadCloser) Read(p []byte) (int, error) {
	return m.reader.Read(p)
}

// Closer is the interface that wraps the basic Close method for multiReadCloser.
func (m *multiReadCloser) Close() error {
	var merr error
	for i := range m.closers {
		if err := m.closers[i].Close(); err != nil {
			merr = multierror.Append(merr, err)
		}
	}

	return merr
}
