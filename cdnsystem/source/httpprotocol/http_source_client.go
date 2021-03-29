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

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/pkg/structure/maputils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"
)

const (
	HttpClient  = "http"
	HttpsClient = "https"
)

func init() {
	httpSourceClient := NewHttpSourceClient()
	source.Register(HttpClient, httpSourceClient)
	source.Register(HttpsClient, httpSourceClient)
}

// NewHttpSourceClient returns a new HttpSourceClient.
func NewHttpSourceClient() source.ResourceClient {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &httpSourceClient{
		httpClient: &http.Client{
			Transport: transport,
		},
	}
}

// httpSourceClient is an implementation of the interface of SourceClient.
type httpSourceClient struct {
	httpClient *http.Client
}

// GetContentLength get length of source
// return -l if request fail
// return -1 if response status is not StatusOK and StatusPartialContent
func (client *httpSourceClient) GetContentLength(url string, header map[string]string) (int64, error) {
	resp, err := client.requestWithHeader(http.MethodGet, url, header, 4*time.Second)
	if err != nil {
		return -1, errors.Wrapf(cdnerrors.ErrURLNotReachable, "get http header meta data failed:%v", err)
	}
	// todo 待讨论，这里如果是其他状态码是否要加入到 ErrURLNotReachable 中,如果不加入会下载404/频繁下载403
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		// todo 这种情况是否要和 err的情况作区分，类似于提出一种其他的错误类型用于表示这种错误是可以与url进行交互，但是状态码不符合预期
		return -1, errors.Wrapf(cdnerrors.ErrURLNotReachable, "get http file length failed, unexpected code: %d", resp.StatusCode)
	}
	return resp.ContentLength, nil
}

// IsSupportRange checks if the source url support partial requests.
func (client *httpSourceClient) IsSupportRange(url string, header map[string]string) (bool, error) {
	// set header: header is a reference to map, should not change it
	copied := maputils.DeepCopyMap(nil, header)
	copied[headers.Range] = "bytes=0-0"

	// send request
	resp, err := client.requestWithHeader(http.MethodHead, url, copied, 4*time.Second)
	if err != nil {
		return false, err
	}
	return resp.StatusCode == http.StatusPartialContent, nil
}

// todo 考虑 expire，类似访问baidu网页是没有last-modified的
// IsExpired checks if a resource received or stored is the same.
func (client *httpSourceClient) IsExpired(url string, header, expireInfo map[string]string) (bool, error) {
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
	resp, err := client.requestWithHeader(http.MethodHead, url, copied, 4*time.Second)
	if err != nil {
		// 如果获取失败，则认为没有过期，防止打爆源
		return false, err
	}
	return resp.StatusCode != http.StatusNotModified, nil
}

// Download downloads the file from the original address
func (client *httpSourceClient) Download(url string, header map[string]string) (io.ReadCloser, map[string]string, error) {
	resp, err := client.requestWithHeader(http.MethodGet, url, header, 0)
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

func (client *httpSourceClient) requestWithHeader(method string, url string, header map[string]string, timeout time.Duration) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header.Add(k, v)
	}
	if timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		req = req.WithContext(ctx)

	}
	return client.httpClient.Do(req)
}
