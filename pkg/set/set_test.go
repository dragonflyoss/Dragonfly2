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
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

const N = 1000

func TestSetAdd(t *testing.T) {
	tests := []struct {
		name   string
		value  interface{}
		expect func(t *testing.T, ok bool, s Set, value interface{})
	}{
		{
			name:  "add value succeeded",
			value: "foo",
			expect: func(t *testing.T, ok bool, s Set, value interface{}) {
				assert := assert.New(t)
				assert.Equal(ok, true)
				assert.Equal(s.Values(), []interface{}{value})
			},
		},
		{
			name:  "add value failed",
			value: "foo",
			expect: func(t *testing.T, _ bool, s Set, value interface{}) {
				ok := s.Add("foo")
				assert := assert.New(t)
				assert.Equal(ok, false)
				assert.Equal(s.Values(), []interface{}{value})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSet()
			tc.expect(t, s.Add(tc.value), s, tc.value)
		})
	}
}

func TestSetAdd_Concurrent(t *testing.T) {
	runtime.GOMAXPROCS(4)

	s := NewSet()
	nums := rand.Perm(N)

	var wg sync.WaitGroup
	wg.Add(len(nums))
	for i := 0; i < len(Perm); i++ {
		go func(i int) {
			s.Add(i)
			wg.Done()
		}(i)
	}

	wg.Wait()
	for _, n := range Perm {
		if !s.Contains(n) {
			t.Errorf("Set is missing element: %v", i)
		}
	}
}
