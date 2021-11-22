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
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	rangers "d7y.io/dragonfly/v2/pkg/util/rangeutils"
)

var _ ResourceClient = (*clientManager)(nil)
var _ ClientManager = (*clientManager)(nil)

// ResourceClient supply apis that interact with the source.
type ResourceClient interface {

	// GetContentLength get length of resource content
	// return -l if request fail
	// return task.IllegalSourceFileLen if response status is not StatusOK and StatusPartialContent
	GetContentLength(ctx context.Context, url string, header RequestHeader, rang *rangers.Range) (int64, error)

	// IsSupportRange checks if resource supports breakpoint continuation
	IsSupportRange(ctx context.Context, url string, header RequestHeader) (bool, error)

	// IsExpired checks if a resource received or stored is the same.
	IsExpired(ctx context.Context, url string, header RequestHeader, expireInfo map[string]string) (bool, error)

	// Download downloads from source
	Download(ctx context.Context, url string, header RequestHeader, rang *rangers.Range) (io.ReadCloser, error)

	// DownloadWithResponseHeader download from source with responseHeader
	DownloadWithResponseHeader(ctx context.Context, url string, header RequestHeader, rang *rangers.Range) (io.ReadCloser, ResponseHeader, error)

	// GetLastModifiedMillis gets last modified timestamp milliseconds of resource
	GetLastModifiedMillis(ctx context.Context, url string, header RequestHeader) (int64, error)
}

type ClientManager interface {
	ResourceClient
	Register(schema string, resourceClient ResourceClient)
	UnRegister(schema string)
}

type clientManager struct {
	sync.RWMutex
	clients map[string]ResourceClient
}

var _defaultManager = NewManager()

func (m *clientManager) GetContentLength(ctx context.Context, url string, header RequestHeader, rang *rangers.Range) (int64, error) {
	sourceClient, err := m.getSourceClient(url)
	if err != nil {
		return -1, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return sourceClient.GetContentLength(ctx, url, header, rang)
}

func (m *clientManager) IsSupportRange(ctx context.Context, url string, header RequestHeader) (bool, error) {
	sourceClient, err := m.getSourceClient(url)
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

func (m *clientManager) IsExpired(ctx context.Context, url string, header RequestHeader, expireInfo map[string]string) (bool, error) {
	sourceClient, err := m.getSourceClient(url)
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

func (m *clientManager) Download(ctx context.Context, url string, header RequestHeader, rang *rangers.Range) (io.ReadCloser, error) {
	sourceClient, err := m.getSourceClient(url)
	if err != nil {
		return nil, err
	}
	return sourceClient.Download(ctx, url, header, rang)
}

func (m *clientManager) DownloadWithResponseHeader(ctx context.Context, url string, header RequestHeader, rang *rangers.Range) (io.ReadCloser, ResponseHeader,
	error) {
	sourceClient, err := m.getSourceClient(url)
	if err != nil {
		return nil, nil, err
	}
	return sourceClient.DownloadWithResponseHeader(ctx, url, header, rang)
}

func (m *clientManager) GetLastModifiedMillis(ctx context.Context, url string, header RequestHeader) (int64, error) {
	sourceClient, err := m.getSourceClient(url)
	if err != nil {
		return -1, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return sourceClient.GetLastModifiedMillis(ctx, url, header)
}

func NewManager() ClientManager {
	return &clientManager{
		clients: make(map[string]ResourceClient),
	}
}

func (m *clientManager) Register(schema string, resourceClient ResourceClient) {
	if client, ok := m.clients[strings.ToLower(schema)]; ok {
		logger.Infof("replace client %#v with %#v for schema %s", client, resourceClient, schema)
	}
	m.clients[strings.ToLower(schema)] = resourceClient
}

func Register(schema string, resourceClient ResourceClient) {
	_defaultManager.Register(schema, resourceClient)
}

func (m *clientManager) UnRegister(schema string) {
	if client, ok := m.clients[strings.ToLower(schema)]; ok {
		logger.Infof("remove client %#v for schema %s", client, schema)
	}
	delete(m.clients, strings.ToLower(schema))
}

func UnRegister(schema string) {
	_defaultManager.UnRegister(schema)
}

func GetContentLength(ctx context.Context, url string, header RequestHeader) (int64, error) {
	var httpRange *rangers.Range
	if header.Get(headers.Range) != "" {
		var err error
		httpRange, err = rangers.ParseHTTPRange(header.Get(headers.Range))
		if err != nil {
			return -1, err
		}
	}
	return _defaultManager.GetContentLength(ctx, url, header, httpRange)
}

func IsSupportRange(ctx context.Context, url string, header RequestHeader) (bool, error) {
	return _defaultManager.IsSupportRange(ctx, url, header)
}

func IsExpired(ctx context.Context, url string, header RequestHeader, expireInfo map[string]string) (bool, error) {
	return _defaultManager.IsExpired(ctx, url, header, expireInfo)
}

func Download(ctx context.Context, url string, header RequestHeader) (io.ReadCloser, error) {
	var httpRange *rangers.Range
	if header.Get(headers.Range) != "" {
		var err error
		httpRange, err = rangers.ParseHTTPRange(header.Get(headers.Range))
		if err != nil {
			return nil, err
		}
	}
	return _defaultManager.Download(ctx, url, header, httpRange)
}

func DownloadWithResponseHeader(ctx context.Context, url string, header RequestHeader) (io.ReadCloser, ResponseHeader, error) {

	var httpRange *rangers.Range
	if header.Get(headers.Range) != "" {
		var err error
		httpRange, err = rangers.ParseHTTPRange(header.Get(headers.Range))
		if err != nil {
			return nil, nil, err
		}
	}
	return _defaultManager.DownloadWithResponseHeader(ctx, url, header, httpRange)
}

func GetLastModifiedMillis(ctx context.Context, url string, header RequestHeader) (int64, error) {
	return _defaultManager.GetLastModifiedMillis(ctx, url, header)
}

// getSourceClient get a source client from source manager with specified schema.
func (m *clientManager) getSourceClient(rawURL string) (ResourceClient, error) {
	logger.Debugf("current clients: %#v", m.clients)
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	m.RLock()
	client, ok := m.clients[strings.ToLower(parsedURL.Scheme)]
	m.RUnlock()
	if !ok || client == nil {
		client, err = m.loadSourcePlugin(strings.ToLower(parsedURL.Scheme))
		if err == nil && client != nil {
			return client, nil
		}
		return nil, errors.Errorf("can not find client for supporting url %s, clients:%v", rawURL, m.clients)
	}
	return client, nil
}

func (m *clientManager) loadSourcePlugin(schema string) (ResourceClient, error) {
	m.Lock()
	defer m.Unlock()
	// double check
	client, ok := m.clients[schema]
	if ok {
		return client, nil
	}

	client, err := LoadPlugin(schema)
	if err != nil {
		return nil, err
	}
	m.clients[schema] = client
	return client, nil
}
