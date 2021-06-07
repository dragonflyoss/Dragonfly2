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

package safe

import (
	"fmt"
	"runtime"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func getCallers(r interface{}) string {
	callers := ""
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		callers = callers + fmt.Sprintf("%v:%v\n", file, line)
	}

	return callers
}

// RecoverFromPanic replaces the specified error with an error containing the
// original error, and the call tree when a panic occurs. This enables error
// handlers to handle errors and panics the same way.
func RecoverFromPanic(err ...*error) {
	if r := recover(); r != nil {
		callers := getCallers(r)
		if len(err) > 0 {
			*err[0] = fmt.Errorf("recovered from panic %q. (err=%v) Call stack:\n%v", r, *err[0], callers)
		} else {
			logger.Errorf("recovered from panic %q. Call stack:\n%v", r, callers)
		}
	}
}
