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

package hdfsprotocol

import (
	"context"
	"io"

	"d7y.io/dragonfly.v2/pkg/source"
)

const (
	HDFSClient = "hdfs"
)

func init() {
	source.Register(HDFSClient, NewHDFSSourceClient())
}

// hdfsSourceClient is an implementation of the interface of SourceClient.
type hdfsSourceClient struct {
}

type HDFSSourceClientOption func(p *hdfsSourceClient)

func (h hdfsSourceClient) GetContentLength(ctx context.Context, url string, header source.RequestHeader) (int64, error) {
	panic("implement me")
}

func (h hdfsSourceClient) IsSupportRange(ctx context.Context, url string, header source.RequestHeader) (bool, error) {
	panic("implement me")
}

func (h hdfsSourceClient) IsExpired(ctx context.Context, url string, header source.RequestHeader, expireInfo map[string]string) (bool, error) {
	panic("implement me")
}

func (h hdfsSourceClient) Download(ctx context.Context, url string, header source.RequestHeader) (io.ReadCloser, error) {
	panic("implement me")
}

func (h hdfsSourceClient) DownloadWithResponseHeader(ctx context.Context, url string, header source.RequestHeader) (io.ReadCloser, source.ResponseHeader, error) {
	panic("implement me")
}

func (h hdfsSourceClient) GetLastModifiedMillis(ctx context.Context, url string, header source.RequestHeader) (int64, error) {
	panic("implement me")
}

func NewHDFSSourceClient(opts ...HDFSSourceClientOption) source.ResourceClient {
	sourceClient := &hdfsSourceClient{}
	for i := range opts {
		opts[i](sourceClient)
	}
	return sourceClient
}

var _ source.ResourceClient = (*hdfsSourceClient)(nil)
