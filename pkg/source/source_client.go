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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var (
	// ErrResourceNotReachable represents the url resource is a not reachable.
	ErrResourceNotReachable = errors.New("resource is not reachable")

	// ErrNoClientFound represents no source client to resolve url
	ErrNoClientFound = errors.New("no source client found")
)

// UnexpectedStatusCodeError is returned when a source responds with neither an error
// nor with a status code indicating success.
type UnexpectedStatusCodeError struct {
	allowed []int // The expected stats code returned from source
	got     int   // The actual status code from source
}

// Error implements interface error
func (e UnexpectedStatusCodeError) Error() string {
	var expected []string
	for _, v := range e.allowed {
		expected = append(expected, strconv.Itoa(v))
	}
	return fmt.Sprintf("status code from source is %s; was expecting %s",
		strconv.Itoa(e.got), strings.Join(expected, " or "))
}

// Got is the actual status code returned by source.
func (e UnexpectedStatusCodeError) Got() int {
	return e.got
}

// CheckResponseCode returns UnexpectedStatusError if the given response code is not
// one of the allowed status codes; otherwise nil.
func CheckResponseCode(respCode int, allowed []int) error {
	for _, v := range allowed {
		if respCode == v {
			return nil
		}
	}
	return UnexpectedStatusCodeError{allowed, respCode}
}

func IsResourceNotReachableError(err error) bool {
	return errors.Is(err, ErrResourceNotReachable)
}

func IsNoClientFoundError(err error) bool {
	return errors.Is(err, ErrNoClientFound)
}

const (
	UnKnownSourceFileLen = -2
)

// ResourceClient defines the API interface to interact with source.
type ResourceClient interface {

	// GetContentLength get length of resource content
	// return source.UnKnownSourceFileLen if response status is not StatusOK and StatusPartialContent
	GetContentLength(request *Request) (int64, error)

	// IsSupportRange checks if resource supports breakpoint continuation
	// return false if response status is not StatusPartialContent
	IsSupportRange(request *Request) (bool, error)

	// IsExpired checks if a resource received or stored is the same.
	// return false and non-nil err to prevent the source from exploding if
	// fails to get the result, it is considered that the source has not expired
	IsExpired(request *Request, info *ExpireInfo) (bool, error)

	// Download downloads from source
	Download(request *Request) (io.ReadCloser, error)

	// DownloadWithExpireInfo download from source with expireInfo
	DownloadWithExpireInfo(request *Request) (io.ReadCloser, *ExpireInfo, error)

	// GetLastModified gets last modified timestamp milliseconds of resource
	GetLastModified(request *Request) (int64, error)
}

type ClientManager interface {
	// Register a source client with scheme
	Register(scheme string, resourceClient ResourceClient, adapter requestAdapter, hook ...Hook) error

	// UnRegister a source client from manager
	UnRegister(scheme string)

	// GetClient a source client by scheme
	GetClient(scheme string) (ResourceClient, bool)
}

// clientManager implements the interface ClientManager
type clientManager struct {
	mu      sync.RWMutex
	clients map[string]ResourceClient
}

var _ ClientManager = (*clientManager)(nil)

var _defaultManager = NewManager()

func NewManager() ClientManager {
	return &clientManager{
		clients: make(map[string]ResourceClient),
	}
}

func (m *clientManager) Register(scheme string, resourceClient ResourceClient, adaptor requestAdapter, hooks ...Hook) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if client, ok := m.clients[strings.ToLower(scheme)]; ok {
		if client.(*clientWrapper).rc != resourceClient {
			return errors.Errorf("client with scheme %s already exist, current client: %#v", scheme, client)
		}
	}
	m.clients[strings.ToLower(scheme)] = &clientWrapper{
		adapter: adaptor,
		hooks:   hooks,
		rc:      resourceClient,
	}
	return nil
}

func (m *clientManager) UnRegister(scheme string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if client, ok := m.clients[strings.ToLower(scheme)]; ok {
		logger.Infof("remove client %#v for scheme %s", client, scheme)
	}
	delete(m.clients, strings.ToLower(scheme))
}

func (m *clientManager) GetClient(scheme string) (ResourceClient, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, ok := m.clients[strings.ToLower(scheme)]
	return client, ok
}

func Register(scheme string, resourceClient ResourceClient, adaptor requestAdapter, hooks ...Hook) error {
	return _defaultManager.Register(scheme, resourceClient, adaptor, hooks...)
}

func UnRegister(scheme string) {
	_defaultManager.UnRegister(scheme)
}

type requestAdapter func(request *Request) *Request

// Hook TODO hook
type Hook interface {
	BeforeRequest(request *Request) error
	AfterResponse(response *Response) error
}

type clientWrapper struct {
	adapter requestAdapter
	hooks   []Hook
	rc      ResourceClient
}

func (c *clientWrapper) GetContentLength(request *Request) (int64, error) {
	return c.rc.GetContentLength(c.adapter(request))
}

func (c *clientWrapper) IsSupportRange(request *Request) (bool, error) {
	return c.rc.IsSupportRange(c.adapter(request))
}

func (c *clientWrapper) IsExpired(request *Request, info *ExpireInfo) (bool, error) {
	return c.rc.IsExpired(c.adapter(request), info)
}
func (c *clientWrapper) Download(request *Request) (io.ReadCloser, error) {
	return c.rc.Download(c.adapter(request))
}

func (c *clientWrapper) DownloadWithExpireInfo(request *Request) (io.ReadCloser, *ExpireInfo, error) {
	return c.rc.DownloadWithExpireInfo(c.adapter(request))
}

func (c *clientWrapper) GetLastModified(request *Request) (int64, error) {
	return c.rc.GetLastModified(c.adapter(request))
}

func GetContentLength(request *Request) (int64, error) {
	client, ok := _defaultManager.GetClient(request.URL.Scheme)
	if !ok {
		return UnKnownSourceFileLen, errors.Wrapf(ErrNoClientFound, "scheme: %s", request.URL.Scheme)
	}
	if _, ok := request.Context().Deadline(); !ok {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		request = request.WithContext(ctx)
		defer cancel()
	}
	return client.GetContentLength(request)
}

func IsSupportRange(request *Request) (bool, error) {
	client, ok := _defaultManager.GetClient(request.URL.Scheme)
	if !ok {
		return false, errors.Wrapf(ErrNoClientFound, "scheme: %s", request.URL.Scheme)
	}
	if _, ok := request.Context().Deadline(); !ok {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		request = request.WithContext(ctx)
		defer cancel()
	}
	if request.Header.get(Range) == "" {
		request.Header.Add(Range, "0-0")
	}
	return client.IsSupportRange(request)
}

func IsExpired(request *Request, info *ExpireInfo) (bool, error) {
	client, ok := _defaultManager.GetClient(request.URL.Scheme)
	if !ok {
		return false, errors.Wrapf(ErrNoClientFound, "scheme: %s", request.URL.Scheme)
	}
	if _, ok := request.Context().Deadline(); !ok {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		request = request.WithContext(ctx)
		defer cancel()
	}
	return client.IsExpired(request, info)
}

func GetLastModified(request *Request) (int64, error) {
	client, ok := _defaultManager.GetClient(request.URL.Scheme)
	if !ok {
		return -1, errors.Wrapf(ErrNoClientFound, "scheme: %s", request.URL.Scheme)
	}
	if _, ok := request.Context().Deadline(); !ok {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		request = request.WithContext(ctx)
		defer cancel()
	}
	return client.GetLastModified(request)
}

func Download(request *Request) (io.ReadCloser, error) {
	client, ok := _defaultManager.GetClient(request.URL.Scheme)
	if !ok {
		return nil, errors.Wrapf(ErrNoClientFound, "scheme: %s", request.URL.Scheme)
	}
	return client.Download(request)
}

func DownloadWithExpireInfo(request *Request) (io.ReadCloser, *ExpireInfo, error) {
	client, ok := _defaultManager.GetClient(request.URL.Scheme)
	if !ok {
		return nil, nil, errors.Wrapf(ErrNoClientFound, "scheme: %s", request.URL.Scheme)
	}
	return client.DownloadWithExpireInfo(request)
}

// getSourceClient get a source client from source manager with specified schema.
func (m *clientManager) getSourceClient(rawURL string) (ResourceClient, error) {
	logger.Debugf("current clients: %#v", m.clients)
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	client, ok := m.clients[strings.ToLower(parsedURL.Scheme)]
	m.mu.RUnlock()
	if !ok || client == nil {
		client, err = m.loadSourcePlugin(strings.ToLower(parsedURL.Scheme))
		if err == nil && client != nil {
			return client, nil
		}
		return nil, errors.Errorf("can not find client for supporting url %s, clients:%v", rawURL, m.clients)
	}
	return client, nil
}

func (m *clientManager) loadSourcePlugin(scheme string) (ResourceClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// double check
	client, ok := m.clients[scheme]
	if ok {
		return client, nil
	}

	client, err := LoadPlugin(scheme)
	if err != nil {
		return nil, err
	}
	m.clients[scheme] = client
	return client, nil
}
