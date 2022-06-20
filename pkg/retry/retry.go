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

package retry

import (
	"context"
	"time"

	"d7y.io/dragonfly/v2/pkg/math"
)

func Run(ctx context.Context,
	f func() (data interface{}, cancel bool, err error),
	initBackoff float64,
	maxBackoff float64,
	maxAttempts int,
	cause error) (interface{}, bool, error) {
	var (
		res    interface{}
		cancel bool
	)
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			time.Sleep(math.RandBackoff(initBackoff, maxBackoff, 2.0, i))
		}

		res, cancel, cause = f()
		if cause == nil || cancel {
			break
		}
		select {
		case <-ctx.Done():
			return nil, cancel, ctx.Err()
		default:
		}
	}

	return res, cancel, cause
}
