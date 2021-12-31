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

package util

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"d7y.io/dragonfly/v2/scheduler/entity"
)

// Download tiny file from cdn
func DownloadTinyFile(ctx context.Context, task *entity.Task, peer *entity.Peer) ([]byte, error) {
	// Download url: http://${host}:${port}/download/${taskIndex}/${taskID}?peerId=scheduler;
	url := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:%d", peer.Host.IP, peer.Host.DownloadPort),
		Path:     fmt.Sprintf("download/%s/%s", task.ID[:3], task.ID),
		RawQuery: "peerId=scheduler",
	}

	peer.Log.Infof("download tiny file url: %#v", url)

	resp, err := http.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}
