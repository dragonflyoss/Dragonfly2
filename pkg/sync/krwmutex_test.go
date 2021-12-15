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

package sync

import (
	"testing"
	"time"
)

func TestKrwmutex_Lock(t *testing.T) {
	k := NewKrwmutex()
	done := time.After(time.Second)

	k.Lock("foo")
	foo := make(chan struct{})
	go func() {
		k.Lock("foo")
		close(foo)
	}()

	k.Lock("bar")
	bar := make(chan struct{})
	go func() {
		k.Lock("bar")
		close(bar)
	}()

	k.Lock("baz")
	baz := make(chan struct{})
	go func() {
		k.Lock("baz")
		close(baz)
	}()

	select {
	case <-bar:
	case <-baz:
	case <-foo:
	case <-done:
		t.Fatal("kmutex is not unlocked")
	default:
	}

	k.Unlock("foo")
	select {
	case <-bar:
	case <-baz:
	case <-done:
		t.Fatal("bar and baz are not unlocked")
	case <-foo:
		k.Unlock("foo")
	}

	k.Unlock("bar")
	select {
	case <-foo:
	case <-baz:
	case <-done:
		t.Fatal("foo and baz are not unlocked")
	case <-bar:
		k.Unlock("bar")
	}

	k.Unlock("baz")
	select {
	case <-foo:
	case <-bar:
	case <-done:
		t.Fatal("foo and bar are not unlocked")
	case <-baz:
		k.Unlock("baz")
	}
}

func TestKrwmutex_RLock(t *testing.T) {
	k := NewKrwmutex()
	done := time.After(time.Second)

	k.Lock("foo")
	foo := make(chan struct{})
	go func() {
		k.RLock("foo")
		close(foo)
	}()

	k.Lock("bar")
	bar := make(chan struct{})
	go func() {
		k.RLock("bar")
		close(bar)
	}()

	k.Lock("baz")
	baz := make(chan struct{})
	go func() {
		k.RLock("baz")
		close(baz)
	}()

	select {
	case <-bar:
	case <-baz:
	case <-foo:
	case <-done:
		t.Fatal("kmutex is not unlocked")
	default:
	}

	k.Unlock("foo")
	select {
	case <-bar:
	case <-baz:
	case <-done:
		t.Fatal("bar and baz are not unlocked")
	case <-foo:
		k.RUnlock("foo")
	}

	k.Unlock("bar")
	select {
	case <-foo:
	case <-baz:
	case <-done:
		t.Fatal("foo and baz are not unlocked")
	case <-bar:
		k.RUnlock("bar")
	}

	k.Unlock("baz")
	select {
	case <-foo:
	case <-bar:
	case <-done:
		t.Fatal("foo and bar are not unlocked")
	case <-baz:
		k.RUnlock("baz")
	}
}
