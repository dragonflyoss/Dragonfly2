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

package set

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetAdd(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		expect func(t *testing.T, ok bool, s Set, value any)
	}{
		{
			name:  "add value",
			value: "foo",
			expect: func(t *testing.T, ok bool, s Set, value any) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(s.Values(), []any{value})
			},
		},
		{
			name:  "add value failed",
			value: "foo",
			expect: func(t *testing.T, _ bool, s Set, value any) {
				assert := assert.New(t)
				ok := s.Add("foo")
				assert.Equal(ok, false)
				assert.Equal(s.Values(), []any{value})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			tc.expect(t, s.Add(tc.value), s, tc.value)
		})
	}
}

func TestSetDelete(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		expect func(t *testing.T, s Set, value any)
	}{
		{
			name:  "delete value",
			value: "foo",
			expect: func(t *testing.T, s Set, value any) {
				assert := assert.New(t)
				s.Delete(value)
				assert.Equal(s.Len(), uint(0))
			},
		},
		{
			name:  "delete value does not exist",
			value: "foo",
			expect: func(t *testing.T, s Set, _ any) {
				assert := assert.New(t)
				s.Delete("bar")
				assert.Equal(s.Len(), uint(1))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			s.Add(tc.value)
			tc.expect(t, s, tc.value)
		})
	}
}

func TestSetContains(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		expect func(t *testing.T, s Set, value any)
	}{
		{
			name:  "contains value",
			value: "foo",
			expect: func(t *testing.T, s Set, value any) {
				assert := assert.New(t)
				assert.Equal(s.Contains(value), true)
			},
		},
		{
			name:  "contains value does not exist",
			value: "foo",
			expect: func(t *testing.T, s Set, _ any) {
				assert := assert.New(t)
				assert.Equal(s.Contains("bar"), false)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			s.Add(tc.value)
			tc.expect(t, s, tc.value)
		})
	}
}

func TestSetLen(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s Set)
	}{
		{
			name: "get length",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				s.Add("foo")
				assert.Equal(s.Len(), uint(1))
			},
		},
		{
			name: "get empty set length",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				assert.Equal(s.Len(), uint(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			tc.expect(t, s)
		})
	}
}

func TestSetValues(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s Set)
	}{
		{
			name: "get values",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				s.Add("foo")
				assert.Equal(s.Values(), []any{"foo"})
			},
		},
		{
			name: "get empty values",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				assert.Equal(s.Values(), []any(nil))
			},
		},
		{
			name: "get multi values",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				s.Add("foo")
				s.Add("bar")
				assert.Contains(s.Values(), "bar")
				assert.Contains(s.Values(), "foo")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			tc.expect(t, s)
		})
	}
}

func TestSetRange(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s Set)
	}{
		{
			name: "range values",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				s.Add("foo")
				s.Range(func(v any) bool {
					assert.Equal(v, "foo")
					return true
				})
			},
		},
		{
			name: "range failed",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				s.Add("foo")
				s.Add("bar")
				s.Range(func(v any) bool {
					assert.Equal(s.Contains(v), true)
					return false
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			tc.expect(t, s)
		})
	}
}

func TestSetClear(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, s Set)
	}{
		{
			name: "clear empty set",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				s.Clear()
				assert.Equal(s.Values(), []any(nil))
			},
		},
		{
			name: "clear set",
			expect: func(t *testing.T, s Set) {
				assert := assert.New(t)
				assert.Equal(s.Add("foo"), true)
				s.Clear()
				assert.Equal(s.Values(), []any(nil))
				assert.Equal(s.Add("foo"), true)
				assert.Equal(s.Values(), []any{"foo"})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			tc.expect(t, s)
		})
	}
}
