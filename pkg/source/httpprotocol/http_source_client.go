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
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/structure/maputils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"
)

const (
	HTTPClient  = "http"
	HTTPSClient = "https"
)

var defaultHTTPClient *http.Client
var _ source.ResourceClient = (*httpSourceClient)(nil)

func init() {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	defaultHTTPClient = &http.Client{
		Transport: transport,
	}
	httpSourceClient := NewHTTPSourceClient()
	source.Register(HTTPClient, httpSourceClient)
	source.Register(HTTPSClient, httpSourceClient)
}

// httpSourceClient is an implementation of the interface of source.ResourceClient.
type httpSourceClient struct {
	httpClient *http.Client
}

// NewHTTPSourceClient returns a new HTTPSourceClientOption.
func NewHTTPSourceClient(opts ...HTTPSourceClientOption) source.ResourceClient {
	client := &httpSourceClient{
		httpClient: defaultHTTPClient,
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

func (client *httpSourceClient) GetContentLength(ctx context.Context, url string, header source.Header) (int64, error) {
	resp, err := client.doRequest(ctx, http.MethodGet, url, header)
	if err != nil {
		return -1, errors.Wrapf(cdnerrors.ErrURLNotReachable, "get http header meta data failed: %v", err)
	}
	resp.Body.Close()
	// todo Here if other status codes should be added to ErrURLNotReachable, if not, it will be downloaded frequently for 404 or 403
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		// todo Whether this situation should be distinguished from the err situation, similar to proposing another error type to indicate that this  error can interact with the URL, but the status code does not meet expectations
		return -1, errors.Wrapf(cdnerrors.ErrURLNotReachable, "failed to get http resource length, unexpected code: %d", resp.StatusCode)
	}
	return resp.ContentLength, nil
}

func (client *httpSourceClient) IsSupportRange(ctx context.Context, url string, header source.Header) (bool, error) {
	copied := maputils.DeepCopyMap(nil, header)
	copied[headers.Range] = "bytes=0-0"

	resp, err := client.doRequest(ctx, http.MethodGet, url, copied)
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusPartialContent, nil
}

// todo Consider the situation where there is no last-modified such as baidu
func (client *httpSourceClient) IsExpired(ctx context.Context, url string, header source.Header, expireInfo map[string]string) (bool, error) {
	lastModified := timeutils.UnixMillis(expireInfo[headers.LastModified])

	eTag := expireInfo[headers.ETag]
	if lastModified <= 0 && stringutils.IsBlank(eTag) {
		return true, nil
	}

	// set header: header is a reference to map, should not change it
	copied := maputils.DeepCopyMap(nil, header)
	if lastModified > 0 {
		copied[headers.IfModifiedSince] = expireInfo[headers.LastModified]
	}
	if !stringutils.IsBlank(eTag) {
		copied[headers.IfNoneMatch] = eTag
	}

	// send request
	resp, err := client.doRequest(ctx, http.MethodGet, url, copied)
	if err != nil {
		// If it fails to get the result, it is considered that the source has not expired, to prevent the source from exploding
		return false, err
	}
	resp.Body.Close()
	return resp.StatusCode != http.StatusNotModified, nil
}

func (client *httpSourceClient) Download(ctx context.Context, url string, header source.Header) (io.ReadCloser, error) {
	resp, err := client.doRequest(ctx, http.MethodGet, url, header)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		return resp.Body, nil
	}
	resp.Body.Close()
	return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

func (client *httpSourceClient) DownloadWithExpire(ctx context.Context, url string, header source.Header) (io.ReadCloser, map[string]string, error) {
	resp, err := client.doRequest(ctx, http.MethodGet, url, header)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		expireInfo := map[string]string{
			headers.LastModified: resp.Header.Get(headers.LastModified),
			headers.ETag:         resp.Header.Get(headers.ETag),
		}
		return resp.Body, expireInfo, nil
	}
	resp.Body.Close()
	return nil, nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

func (client *httpSourceClient) GetExpireInfo(ctx context.Context, url string, header source.Header) (map[string]string, error) {
	resp, err := client.doRequest(ctx, http.MethodGet, url, header)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		expireInfo := map[string]string{
			headers.LastModified: resp.Header.Get(headers.LastModified),
			headers.ETag:         resp.Header.Get(headers.ETag),
		}
		return expireInfo, nil
	}
	return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

func (client *httpSourceClient) doRequest(ctx context.Context, method string, url string, header source.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header.Add(k, v)
	}
	return client.httpClient.Do(req)
}
