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

package cdnutil

import (
	"fmt"
	"testing"

	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

func TestGenCdnPeerID(t *testing.T) {
	var taskID = "123456"
	if got, want := GenCDNPeerID(taskID), fmt.Sprint(iputils.HostName, "-", taskID, "_CDN"); got != want {
		t.Errorf("GenCdnPeerID() = %v, want %v", got, want)
	}
}

func TestComputePieceSize(t *testing.T) {
	var length int64 = 2002 * 1024 * 1024
	if size := ComputePieceSize(length); size == 4*1024*1024 {
		fmt.Printf(string("length <= 0 || length <= 200*1024*1024"))
	} else if size := ComputePieceSize(length); size == 15*1024*1024 {
		fmt.Printf(string("mapsize <= 15*1024*1024"))
	} else {
		fmt.Printf(string("输出mpSize"))
	}
}
