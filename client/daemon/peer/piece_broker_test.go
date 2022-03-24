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

package peer

import (
	"sync"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func TestBroker(t *testing.T) {
	var testCases = []struct {
		name   string
		total  int32
		pubSeq []int32
		desire []*PieceInfo
	}{
		{
			name:   "order publish",
			total:  3,
			pubSeq: []int32{0, 1, 2},
			desire: []*PieceInfo{
				{
					Num:        0,
					OrderedNum: 0,
					Finished:   false,
				},
				{
					Num:        1,
					OrderedNum: 1,
					Finished:   false,
				},
				{
					Num:        2,
					OrderedNum: 2,
					Finished:   true,
				},
			},
		},
		{
			name:   "partial order publish",
			total:  3,
			pubSeq: []int32{0, 2, 1},
			desire: []*PieceInfo{
				{
					Num:        0,
					OrderedNum: 0,
					Finished:   false,
				},
				{
					Num:        2,
					OrderedNum: 0,
					Finished:   false,
				},
				{
					Num:        1,
					OrderedNum: 2,
					Finished:   true,
				},
			},
		},
		{
			name:   "inverted order publish",
			total:  3,
			pubSeq: []int32{2, 1, 0},
			desire: []*PieceInfo{
				{
					Num:        2,
					OrderedNum: -1,
					Finished:   false,
				},
				{
					Num:        1,
					OrderedNum: -1,
					Finished:   false,
				},
				{
					Num:        0,
					OrderedNum: 2,
					Finished:   true,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)
			broker := newPieceBroker()
			go broker.Start()
			ch := broker.Subscribe()
			done := make(chan struct{})
			var received []*PieceInfo

			wg := sync.WaitGroup{}
			wg.Add(len(tc.pubSeq))
			go func() {
				for {
					select {
					case <-done:
						return
					case info := <-ch:
						received = append(received, info)
						wg.Done()
					}
				}
			}()
			var sent int32
			for _, n := range tc.pubSeq {
				sent++
				var finished bool
				if sent == tc.total {
					finished = true
				}
				broker.Publish(&PieceInfo{
					Num:      n,
					Finished: finished,
				})
			}
			wg.Wait()
			broker.Stop()
			close(done)
			assert.Equal(tc.desire, received)
		})
	}
}
