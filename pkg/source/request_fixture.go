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

package source

import (
	"fmt"

	"go.uber.org/mock/gomock"
)

// RequestEq for gomock
func RequestEq(url string) gomock.Matcher {
	return requestMatcher{url}
}

type requestMatcher struct {
	url string
}

// Matches returns whether x is a match.
func (req requestMatcher) Matches(x any) bool {
	return x.(*Request).URL.String() == req.url
}

// String describes what the matcher matches.
func (req requestMatcher) String() string {
	return fmt.Sprintf("is equal to %v (%T)", req.url, req.url)
}
