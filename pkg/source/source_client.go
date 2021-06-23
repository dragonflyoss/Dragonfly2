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
//go:generate mockgen -destination ./mock/mock_source_client.go -package mock d7y.io/dragonfly/v2/pkg/source ResourceClient

package source

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var _ ResourceClient = (*ClientManagerImpl)(nil)
var _ ClientManager = (*ClientManagerImpl)(nil)

type Header map[string]string

// ResourceClient supply apis that interact with the source.
type ResourceClient interface {

	// GetContentLength the length of the associated content. The
	// value -1 indicates that the length is unknown. values >= 0 indicate
	// that the given number of bytes may be read from Body.
	// return -l if request fail
	// return -1 if response status is not StatusOK and StatusPartialContent
	GetContentLength(ctx context.Context, url string, header Header) (int64, error)

	// IsSupportRange checks if resource supports breakpoint continuation
	IsSupportRange(ctx context.Context, url string, header Header) (bool, error)

	// IsExpired checks if a resource received or stored is the same.
	IsExpired(ctx context.Context, url string, header Header, expireInfo map[string]string) (bool, error)

	// Download download from source
	Download(ctx context.Context, url string, header Header) (io.ReadCloser, error)

	// DownloadWithExpire download from source and get expire info
	DownloadWithExpire(ctx context.Context, url string, header Header) (io.ReadCloser, map[string]string, error)

	// GetExpireInfo get expire info of resource
	GetExpireInfo(ctx context.Context, url string, header Header) (map[string]string, error)
}

type ClientManager interface {
	ResourceClient
	Register(schema string, resourceClient ResourceClient)
	UnRegister(schema string)
}

type ClientManagerImpl struct {
	clients map[string]ResourceClient
}

var _defaultMgr = &ClientManagerImpl{
	clients: make(map[string]ResourceClient),
}

func (clientMgr *ClientManagerImpl) GetContentLength(ctx context.Context, url string, header Header) (int64, error) {
	sourceClient, err := clientMgr.getSourceClient(url)
	if err != nil {
		return -1, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return sourceClient.GetContentLength(ctx, url, header)
}

func (clientMgr *ClientManagerImpl) IsSupportRange(ctx context.Context, url string, header Header) (bool, error) {
	sourceClient, err := clientMgr.getSourceClient(url)
	if err != nil {
		return false, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return sourceClient.IsSupportRange(ctx, url, header)
}

func (clientMgr *ClientManagerImpl) IsExpired(ctx context.Context, url string, header Header, expireInfo map[string]string) (bool, error) {
	sourceClient, err := clientMgr.getSourceClient(url)
	if err != nil {
		return false, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return sourceClient.IsExpired(ctx, url, header, expireInfo)
}

func (clientMgr *ClientManagerImpl) Download(ctx context.Context, url string, header Header) (io.ReadCloser, error) {
	sourceClient, err := clientMgr.getSourceClient(url)
	if err != nil {
		return nil, err
	}
	return sourceClient.Download(ctx, url, header)
}

func (clientMgr *ClientManagerImpl) DownloadWithExpire(ctx context.Context, url string, header Header) (io.ReadCloser, map[string]string, error) {
	sourceClient, err := clientMgr.getSourceClient(url)
	if err != nil {
		return nil, nil, err
	}
	return sourceClient.DownloadWithExpire(ctx, url, header)
}

func (clientMgr *ClientManagerImpl) GetExpireInfo(ctx context.Context, url string, header Header) (map[string]string, error) {
	sourceClient, err := clientMgr.getSourceClient(url)
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return sourceClient.GetExpireInfo(ctx, url, header)
}

func NewManager() ClientManager {
	return &ClientManagerImpl{
		clients: make(map[string]ResourceClient),
	}
}

func (clientMgr *ClientManagerImpl) Register(schema string, resourceClient ResourceClient) {
	if client, ok := clientMgr.clients[strings.ToLower(schema)]; ok {
		logger.Infof("replace client %#v with %#v for schema %s", client, resourceClient, schema)
	}
	clientMgr.clients[strings.ToLower(schema)] = resourceClient
}

func Register(schema string, resourceClient ResourceClient) {
	_defaultMgr.Register(schema, resourceClient)
}

func (clientMgr *ClientManagerImpl) UnRegister(schema string) {
	if client, ok := clientMgr.clients[strings.ToLower(schema)]; ok {
		logger.Infof("remove client %#v for schema %s", client, schema)
	}
	delete(clientMgr.clients, strings.ToLower(schema))
}

func UnRegister(schema string) {
	_defaultMgr.UnRegister(schema)
}

func GetContentLength(ctx context.Context, url string, header Header) (int64, error) {
	return _defaultMgr.GetContentLength(ctx, url, header)
}

func IsSupportRange(ctx context.Context, url string, header Header) (bool, error) {
	return _defaultMgr.IsSupportRange(ctx, url, header)
}

func IsExpired(ctx context.Context, url string, header Header, expireInfo map[string]string) (bool, error) {
	return _defaultMgr.IsExpired(ctx, url, header, expireInfo)
}

func Download(ctx context.Context, url string, header Header) (io.ReadCloser, error) {
	return _defaultMgr.Download(ctx, url, header)
}

func DownloadWithExpire(ctx context.Context, url string, header Header) (io.ReadCloser, map[string]string, error) {
	return _defaultMgr.DownloadWithExpire(ctx, url, header)
}

// getSourceClient get a source client from source manager with specified schema.
func (clientMgr *ClientManagerImpl) getSourceClient(rawURL string) (ResourceClient, error) {
	logger.Debugf("current clients:%v", clientMgr.clients)
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	client, ok := clientMgr.clients[strings.ToLower(parsedURL.Scheme)]
	if !ok || client == nil {
		return nil, fmt.Errorf("can not find client for supporting url %s, clients:%v", rawURL, clientMgr.clients)
	}
	return client, nil
}
