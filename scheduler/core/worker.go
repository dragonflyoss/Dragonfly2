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

package core

import "github.com/panjf2000/ants/v2"

type PeerLeaveTask struct {
	peerID string
}

type worker struct {
	pool *ants.Pool
}

func newWorker(workerNum int) (*worker, error) {
	pool, err := ants.NewPool(workerNum)
	if err != nil {
		return nil, err
	}
	return &worker{pool: pool}, nil
}

func (worker *worker) submit(task func()) {
	worker.pool.Submit(task)
}
