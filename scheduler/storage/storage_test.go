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

package storage

import (
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStorage_New(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		options []Option
		expect  func(t *testing.T, s Storage, err error)
	}{
		{
			name:    "new storage",
			baseDir: os.TempDir(),
			options: []Option{},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
				assert.NoError(err)

				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "new storage with maxSize",
			baseDir: os.TempDir(),
			options: []Option{WithMaxSize(100)},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
				assert.NoError(err)

				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "new storage with maxBackups",
			baseDir: os.TempDir(),
			options: []Option{WithMaxBackups(100)},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "storage")
				assert.NoError(err)

				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "new storage failed",
			baseDir: "/foo",
			options: []Option{WithMaxBackups(100)},
			expect: func(t *testing.T, s Storage, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.options...)
			tc.expect(t, s, err)
		})
	}
}

func TestStorage_Create(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		options []Option
		mock    func(s Storage)
		expect  func(t *testing.T, s Storage)
	}{
		{
			name:    "create record",
			baseDir: os.TempDir(),
			options: []Option{},
			mock:    func(s Storage) {},
			expect: func(t *testing.T, s Storage) {
				assert := assert.New(t)
				err := s.Create(Record{})
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir, tc.options...)
			if err != nil {
				t.Fatal(err)
			}

			tc.expect(t, s)
			if err := s.Clear(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
