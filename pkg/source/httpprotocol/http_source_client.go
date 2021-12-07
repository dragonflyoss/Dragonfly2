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

package httpprotocol

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/go-http-utils/headers"

	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
)

const (
	HTTPClient  = "http"
	HTTPSClient = "https"

	ProxyEnv = "D7Y_SOURCE_PROXY"
)

var _defaultHTTPClient *http.Client
var _ source.ResourceClient = (*httpSourceClient)(nil)

func init() {
	// TODO support customize source client
	var (
		proxy *url.URL
		err   error
	)
	if proxyEnv := os.Getenv(ProxyEnv); len(proxyEnv) > 0 {
		proxy, err = url.Parse(proxyEnv)
		if err != nil {
			fmt.Printf("Back source proxy parse error: %s\n", err)
		}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig.InsecureSkipVerify = true
	transport.DialContext = (&net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext

	if proxy != nil {
		transport.Proxy = http.ProxyURL(proxy)
	}

	_defaultHTTPClient = &http.Client{
		Transport: transport,
	}
	sc := NewHTTPSourceClient()

	if err := source.Register(HTTPClient, sc, Adapter); err != nil {
		panic(err)
	}

	if err := source.Register(HTTPSClient, sc, Adapter); err != nil {
		panic(err)
	}
}

func Adapter(request *source.Request) *source.Request {
	clonedRequest := request.Clone(request.Context())
	if request.Header.Get(source.Range) != "" {
		clonedRequest.Header.Set(headers.Range, fmt.Sprintf("bytes=%s", request.Header.Get(source.Range)))
		clonedRequest.Header.Del(source.Range)
	}
	if request.Header.Get(source.LastModified) != "" {
		clonedRequest.Header.Set(headers.LastModified, request.Header.Get(source.LastModified))
		clonedRequest.Header.Del(source.LastModified)
	}
	if request.Header.Get(source.ETag) != "" {
		clonedRequest.Header.Set(headers.ETag, request.Header.Get(source.ETag))
		clonedRequest.Header.Del(source.ETag)
	}
	return clonedRequest
}

// httpSourceClient is an implementation of the interface of source.ResourceClient.
type httpSourceClient struct {
	httpClient *http.Client
}

// NewHTTPSourceClient returns a new HTTPSourceClientOption.
func NewHTTPSourceClient(opts ...HTTPSourceClientOption) source.ResourceClient {
	return newHTTPSourceClient(opts...)
}

func newHTTPSourceClient(opts ...HTTPSourceClientOption) *httpSourceClient {
	client := &httpSourceClient{
		httpClient: _defaultHTTPClient,
	}
	for i := range opts {
		opts[i](client)
	}
	return client
}

type HTTPSourceClientOption func(p *httpSourceClient)

func WithHTTPClient(client *http.Client) HTTPSourceClientOption {
	return func(sourceClient *httpSourceClient) {
		sourceClient.httpClient = client
	}
}

func (client *httpSourceClient) GetContentLength(request *source.Request) (int64, error) {
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return source.UnKnownSourceFileLen, err
	}
	defer resp.Body.Close()
	err = source.CheckResponseCode(resp.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
	if err != nil {
		return source.UnKnownSourceFileLen, err
	}
	return resp.ContentLength, nil
}

func (client *httpSourceClient) IsSupportRange(request *source.Request) (bool, error) {
	if request.Header.Get(headers.Range) == "" {
		request.Header.Set(headers.Range, "bytes=0-0")
	}
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusPartialContent, nil
}

func (client *httpSourceClient) IsExpired(request *source.Request, info *source.ExpireInfo) (bool, error) {
	if info != nil {
		if request.Header == nil {
			request.Header = source.Header{}
		}
		if info.LastModified != "" {
			request.Header.Set(headers.IfModifiedSince, info.LastModified)
		}
		if info.ETag != "" {
			request.Header.Set(headers.IfNoneMatch, info.ETag)
		}
	}
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	return !(resp.StatusCode == http.StatusNotModified || (resp.Header.Get(headers.ETag) == info.ETag || resp.Header.Get(headers.LastModified) == info.
		LastModified)), nil
}

func (client *httpSourceClient) Download(request *source.Request) (io.ReadCloser, error) {
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return nil, err
	}
	err = source.CheckResponseCode(resp.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

func (client *httpSourceClient) DownloadWithExpireInfo(request *source.Request) (io.ReadCloser, *source.ExpireInfo, error) {
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return nil, nil, err
	}
	err = source.CheckResponseCode(resp.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
	if err != nil {
		resp.Body.Close()
		return nil, nil, err
	}
	return resp.Body, &source.ExpireInfo{
		LastModified: resp.Header.Get(headers.LastModified),
		ETag:         resp.Header.Get(headers.ETag),
	}, nil
}

func (client *httpSourceClient) GetLastModified(request *source.Request) (int64, error) {
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()
	err = source.CheckResponseCode(resp.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
	if err != nil {
		return -1, err
	}
	return timeutils.UnixMillis(resp.Header.Get(headers.LastModified)), nil
}

func (client *httpSourceClient) doRequest(method string, request *source.Request) (*http.Response, error) {
	req, err := http.NewRequestWithContext(request.Context(), method, request.URL.String(), nil)
	if err != nil {
		return nil, err
	}
	for key, values := range request.Header {
		for i := range values {
			req.Header.Add(key, values[i])
		}
	}
	resp, err := client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
