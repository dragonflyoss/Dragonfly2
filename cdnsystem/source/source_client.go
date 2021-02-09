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
	"d7y.io/dragonfly/v2/cdnsystem/types"
)

var clients = make(map[string]ResourceClient)

func Register(schema string, resourceClient ResourceClient) {
	clients[schema] = resourceClient
}

func NewSourceClient() (ResourceClient, error) {
	return &ResourceClientAdaptor{
		clients: clients,
	}, nil
}

// SourceClient supply apis that interact with the source.
type ResourceClient interface {

	// GetContentLength get content length from source
	GetContentLength(url string, headers map[string]string) (int64, error)

	// IsSupportRange checks if source supports breakpoint continuation
	IsSupportRange(url string, headers map[string]string) (bool, error)

	// IsExpired checks if cache is expired
	IsExpired(url string, headers, expireInfo map[string]string) (bool, error)

	// Download download from source
	Download(url string, headers map[string]string) (*types.DownloadResponse, error)
}

type ResourceClientAdaptor struct {
	clients map[string]ResourceClient
}

func (s *ResourceClientAdaptor) GetContentLength(url string, headers map[string]string) (int64, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return -1, err
	}
	return sourceClient.GetContentLength(url, headers)
}

func (s *ResourceClientAdaptor) IsSupportRange(url string, headers map[string]string) (bool, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return false, err
	}
	return sourceClient.IsSupportRange(url, headers)
}

func (s *ResourceClientAdaptor) IsExpired(url string, headers, expireInfo map[string]string) (bool, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return false, err
	}
	return sourceClient.IsExpired(url, headers, expireInfo)
}

func (s *ResourceClientAdaptor) Download(url string, headers map[string]string) (*types.DownloadResponse, error) {
	sourceClient, err := s.getSourceClient(url)
	if err != nil {
		return nil, err
	}
	return sourceClient.Download(url, headers)
}
