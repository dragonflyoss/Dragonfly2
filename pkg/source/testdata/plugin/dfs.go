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

package main

import (
	"bytes"
	"io"

	"d7y.io/dragonfly/v2/pkg/source"
)

var data = "hello world"

var (
	buildCommit = "unknown"
	buildTime   = "unknown"
	vendor      = "d7y"
)

var _ source.ResourceClient = (*client)(nil)

type client struct {
}

func (c *client) GetContentLength(request *source.Request) (int64, error) {
	return int64(len(data)), nil
}

func (c *client) IsSupportRange(request *source.Request) (bool, error) {
	return false, nil
}

func (c *client) IsExpired(request *source.Request, info *source.ExpireInfo) (bool, error) {
	panic("implement me")
}

func (c *client) Download(request *source.Request) (*source.Response, error) {
	return source.NewResponse(io.NopCloser(bytes.NewBufferString(data))), nil
}

func (c *client) GetLastModified(request *source.Request) (int64, error) {
	panic("implement me")
}

func DragonflyPluginInit(option map[string]string) (any, map[string]string, error) {
	return &client{}, map[string]string{
		"type":        "resource",
		"name":        "dfs",
		"scheme":      "dfs",
		"buildCommit": buildCommit,
		"buildTime":   buildTime,
		"vendor":      vendor,
	}, nil
}
