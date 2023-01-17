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
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/go-http-utils/headers"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/source"
)

const (
	HTTPClient  = "http"
	HTTPSClient = "https"
)

var (
	_ source.ResourceClient = (*httpSourceClient)(nil)

	// Syntax:
	//   Content-Range: <unit> <range-start>-<range-end>/<size> -> Done
	//   Content-Range: <unit> <range-start>-<range-end>/* -> Done
	//   Content-Range: <unit> */<size> -> TODO
	contentRangeRegexp            = regexp.MustCompile(`bytes (?P<Start>\d+)-(?P<End>\d+)/(?P<Length>(\d*|\*))`)
	contentRangeRegexpLengthIndex = contentRangeRegexp.SubexpIndex("Length")

	notTemporaryStatusCode = []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusProxyAuthRequired,
	}
)

func init() {
	source.RegisterBuilder(HTTPClient, source.NewPlainResourceClientBuilder(Builder))
	source.RegisterBuilder(HTTPSClient, source.NewPlainResourceClientBuilder(Builder))
}

func Builder(optionYaml []byte) (source.ResourceClient, source.RequestAdapter, []source.Hook, error) {
	var httpClient *http.Client
	httpClient, err := source.ParseToHTTPClient(optionYaml)
	if err != nil {
		return nil, nil, nil, err
	}
	sc := NewHTTPSourceClient(WithHTTPClient(httpClient))
	return sc, Adapter, nil, nil
}

func Adapter(request *source.Request) *source.Request {
	clonedRequest := request.Clone(request.Context())
	if request.Header.Get(source.Range) != "" {
		clonedRequest.Header.Set(headers.Range, fmt.Sprintf("bytes=%s", request.Header.Get(source.Range)))
		clonedRequest.Header.Del(source.Range)
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
	client := &httpSourceClient{}
	for i := range opts {
		opts[i](client)
	}
	if client.httpClient == nil {
		client.httpClient = http.DefaultClient
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
		return source.UnknownSourceFileLen, err
	}
	defer resp.Body.Close()
	err = source.CheckResponseCode(resp.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
	if err != nil {
		return source.UnknownSourceFileLen, err
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

func (client *httpSourceClient) GetMetadata(request *source.Request) (*source.Metadata, error) {
	// clone a new request to avoid change original request
	request = request.Clone(request.Context())
	request.Header.Set(headers.Range, "bytes=0-0")

	// use GET method to get metadata instead of HEAD method
	// some object service like OSS, only sign url with GET method, so did not use HEAD method
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var totalContentLength int64 = -1
	cr := resp.Header.Get(headers.ContentRange)
	if cr != "" {
		matches := contentRangeRegexp.FindStringSubmatch(cr)
		if len(matches) > contentRangeRegexpLengthIndex {
			length := matches[contentRangeRegexpLengthIndex]
			if length != "*" {
				totalContentLength, err = strconv.ParseInt(length, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("convert length from string %q error: %w", length, err)
				}
			}
		}

		// this server supports Range request
		// we can discard the only one byte for reuse underlay connection
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 1))
		if err != io.EOF && err != nil {
			return nil, err
		}
	}

	hdr := source.Header{}
	for k, v := range exportPassThroughHeader(resp.Header) {
		hdr.Set(k, v)
	}
	return &source.Metadata{
		Header:             hdr,
		Status:             resp.Status,
		StatusCode:         resp.StatusCode,
		SupportRange:       resp.StatusCode == http.StatusPartialContent,
		TotalContentLength: totalContentLength,
		Validate: func() error {
			return source.CheckResponseCode(resp.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
		},
		Temporary: detectTemporary(resp.StatusCode),
	}, nil
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

func (client *httpSourceClient) Download(request *source.Request) (*source.Response, error) {
	resp, err := client.doRequest(http.MethodGet, request)
	if err != nil {
		return nil, err
	}
	response := source.NewResponse(
		resp.Body,
		source.WithStatus(resp.StatusCode, resp.Status),
		source.WithValidate(func() error {
			return source.CheckResponseCode(resp.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
		}),
		source.WithTemporary(detectTemporary(resp.StatusCode)),
		source.WithHeader(exportPassThroughHeader(resp.Header)),
		source.WithExpireInfo(
			source.ExpireInfo{
				LastModified: resp.Header.Get(headers.LastModified),
				ETag:         resp.Header.Get(headers.ETag),
			},
		),
	)
	if resp.ContentLength > 0 {
		response.ContentLength = resp.ContentLength
	}
	return response, nil
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

	lastModified := resp.Header.Get(headers.LastModified)
	if lastModified == "" {
		return -1, err
	}

	t, err := time.ParseInLocation(source.TimeFormat, lastModified, time.UTC)
	if err != nil {
		return -1, err
	}

	return t.UnixNano() / time.Millisecond.Nanoseconds(), nil
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

	logger.Debugf("request %s %s header: %#v", method, req.URL.String(), req.Header)
	resp, err := client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func exportPassThroughHeader(header http.Header) map[string]string {
	var ph = map[string]string{}
	for h := range PassThroughHeaders {
		val := header.Get(h)
		if len(val) > 0 {
			ph[h] = val
		}
	}
	return ph
}

func detectTemporary(statusCode int) bool {
	for _, code := range notTemporaryStatusCode {
		if code == statusCode {
			return false
		}
	}
	return true
}
