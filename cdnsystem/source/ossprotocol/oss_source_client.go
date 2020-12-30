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

package ossprotocol

import (
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
)

const ossClient = "oss"

func init() {
	sourceClient, err := newOSSSourceClient()
	if err != nil {

	}
	source.Register(ossClient, sourceClient)
}


// httpSourceClient is an implementation of the interface of SourceClient.
type ossSourceClient struct {
}

func (o ossSourceClient) GetContentLength(url string, headers map[string]string) (int64, error) {
	panic("implement me")
}

func (o ossSourceClient) IsSupportRange(url string, headers map[string]string) (bool, error) {
	panic("implement me")
}

func (o ossSourceClient) IsExpired(url string, headers, expireInfo map[string]string) (bool, error) {
	panic("implement me")
}

func (o ossSourceClient) Download(url string, headers map[string]string, checkCode source.StatusCodeChecker) (*types.DownloadResponse, error) {
	panic("implement me")
}

func newOSSSourceClient() (source.ResourceClient, error) {
	return nil, nil
}
