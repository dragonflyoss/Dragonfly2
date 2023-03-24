/*
 *     Copyright 2023 The Dragonfly Authors
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

package metrics

import (
	"testing"
)

func TestNew(t *testing.T) {
	addr := "localhost:8080"
	server := New(addr)

	if server.Addr != addr {
		t.Errorf("Expected server address to be %s, but got %s", addr, server.Addr)
	}

	if server.Handler == nil {
		t.Error("Expected server handler to not be nil")
	}
}
