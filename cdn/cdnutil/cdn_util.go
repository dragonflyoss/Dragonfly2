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

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/pkg/util/net/iputils"
)

func GenCDNPeerID(taskID string) string {
	return fmt.Sprintf("%s-%s_%s", iputils.HostName, taskID, "CDN")
}

// ComputePieceSize computes the piece size with specified fileLength.
//
// If the fileLength<=0, which means failed to get fileLength
// and then use the DefaultPieceSize.
func ComputePieceSize(length int64) int32 {
	if length <= 0 || length <= 200*1024*1024 {
		return config.DefaultPieceSize
	}

	gapCount := length / int64(100*1024*1024)
	mpSize := (gapCount-2)*1024*1024 + config.DefaultPieceSize
	if mpSize > config.DefaultPieceSizeLimit {
		return config.DefaultPieceSizeLimit
	}
	return int32(mpSize)
}
