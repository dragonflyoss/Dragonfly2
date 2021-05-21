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

package queue

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestQueue(t *testing.T) {
	suite.Run(t, new(QueueTestSuite))
}

type QueueTestSuite struct {
	suite.Suite
}

func (suite *QueueTestSuite) SetupTest() {
	rand.Seed(time.Now().UnixNano())
}

func (suite *QueueTestSuite) TestQueue_infiniteQueue() {
	timeout := 50 * time.Millisecond
	q := NewQueue(0)

	q.Put(nil)
	suite.Equal(q.Len(), 0)

	q.PutTimeout(nil, 0)
	suite.Equal(q.Len(), 0)

	q.Put(0)
	suite.Equal(q.Len(), 1)
	suite.Equal(q.Poll(), 0)
	suite.Equal(q.Len(), 0)

	{ // test Poll
		time.AfterFunc(timeout, func() { q.Put(1) })
		start := time.Now()
		suite.Equal(q.Poll(), 1)
		suite.Equal(time.Since(start) > timeout, true)
	}

	{ // test PollTimeout
		v, ok := q.PollTimeout(0)
		suite.Nil(v)
		suite.Equal(ok, false)

		start := time.Now()
		v, ok = q.PollTimeout(timeout)
		suite.Nil(v)
		suite.Equal(ok, false)
		suite.Equal(time.Since(start) > timeout, true)

		time.AfterFunc(timeout/2, func() { q.Put(1) })
		start = time.Now()
		v, ok = q.PollTimeout(timeout)
		suite.Equal(ok, true)
		suite.Equal(v, 1)
		suite.Equal(time.Since(start) >= timeout/2, true)
		suite.Equal(time.Since(start) < timeout, true)
	}
}
func (suite *QueueTestSuite) TestQueue_infiniteQueue_PollTimeout() {
	timeout := 50 * time.Millisecond
	q := NewQueue(0)

	wg := sync.WaitGroup{}
	var cnt int32
	f := func(i int) {
		if _, ok := q.PollTimeout(timeout); ok {
			atomic.AddInt32(&cnt, 1)
		}
		wg.Done()
	}
	start := time.Now()
	n := 6
	wg.Add(n)
	for i := 0; i < n; i++ {
		go f(i)
	}
	time.AfterFunc(timeout/2, func() {
		for i := 0; i < n-1; i++ {
			q.Put(i)
		}
	})
	wg.Wait()

	suite.Equal(time.Since(start) > timeout, true)
	suite.Equal(cnt, int32(n-1))
}

func (suite *QueueTestSuite) TestQueue_finiteQueue() {
	timeout := 50 * time.Millisecond
	q := NewQueue(2)

	q.Put(nil)
	suite.Equal(q.Len(), 0)

	q.PutTimeout(nil, 0)
	suite.Equal(q.Len(), 0)

	q.Put(1)
	suite.Equal(q.Len(), 1)

	start := time.Now()
	q.PutTimeout(2, timeout)
	q.PutTimeout(3, timeout)
	q.PutTimeout(4, 0)
	suite.Equal(q.Len(), 2)
	suite.Equal(time.Since(start) >= timeout, true)
	suite.Equal(time.Since(start) < 2*timeout, true)

	suite.Equal(q.Poll(), 1)
	suite.Equal(q.Len(), 1)
	suite.Equal(q.Poll(), 2)

	{
		q.PutTimeout(1, 0)
		item, ok := q.PollTimeout(timeout)
		suite.Equal(ok, true)
		suite.Equal(item, 1)

		start = time.Now()
		item, ok = q.PollTimeout(timeout)
		suite.Equal(ok, false)
		suite.Nil(item)
		suite.Equal(time.Since(start) >= timeout, true)

		start = time.Now()
		q.PutTimeout(1, 0)
		item, ok = q.PollTimeout(0)
		suite.Equal(ok, true)
		suite.Equal(item, 1)
		item, ok = q.PollTimeout(0)
		suite.Equal(ok, false)
		suite.Nil(item)
		suite.Equal(time.Since(start) < timeout, true)
	}
}
