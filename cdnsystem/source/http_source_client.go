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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/dferrors"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/httputils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/netutils"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/stringutils"
	"github.com/sirupsen/logrus"
	"net"
	"net/http"
	netUrl "net/url"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
)

// HttpClient is a const of http source client.
const HttpClient = "http"
const HttpsClient = "https"

func init() {
	sourceClient, err := newHttpSourceClient()
	if err != nil {

	}
	Register(HttpClient, sourceClient)
	Register(HttpsClient, sourceClient)
}

// NewHttpSourceClient returns a new HttpSourceClient.
func newHttpSourceClient() (ResourceClient, error) {
	defaultTransport := &http.Transport{
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
	httputils.RegisterProtocolOnTransport(defaultTransport)
	return &httpSourceClient{
		clientMap: &sync.Map{},
		defaultHTTPClient: &http.Client{
			Transport: defaultTransport,
		},
	}, nil
}

// httpSourceClient is an implementation of the interface of SourceClient.
type httpSourceClient struct {
	clientMap         *sync.Map
	defaultHTTPClient *http.Client
}

// RegisterTLSConfig saves tls config into map as http client.
// tlsMap:
// key->host value->*http.Client
func (client *httpSourceClient) RegisterTLSConfig(rawURL string, insecure bool, caBlock []strfmt.Base64) error {
	url, err := netUrl.Parse(rawURL)
	if err != nil {
		return err
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecure,
	}
	appendSuccess := false
	roots := x509.NewCertPool()
	for _, caBytes := range caBlock {
		appendSuccess = appendSuccess || roots.AppendCertsFromPEM(caBytes)
	}
	if appendSuccess {
		tlsConfig.RootCAs = roots
	}

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
		TLSClientConfig:       tlsConfig,
	}

	httputils.RegisterProtocolOnTransport(transport)

	client.clientMap.Store(url.Host, &http.Client{
		Transport: transport,
	})
	return nil
}

// GetContentLength sends a head request to get file length.
func (client *httpSourceClient) getHTTPFileLength(url string, headers map[string]string) (int64, int, error) {
	// send request
	resp, err := client.httpWithHeaders(http.MethodGet, url, headers, 4*time.Second)
	if err != nil {
		return 0, 0, err
	}
	resp.Body.Close()

	return resp.ContentLength, resp.StatusCode, nil
}

// GetContentLength get length of source
// return -l if request fail
// return -1 if response status is StatusUnauthorized or StatusProxyAuthRequired
// return -1 if response status is not StatusOK and StatusPartialContent
func (client *httpSourceClient) GetContentLength(url string, headers map[string]string) (int64, error) {
	fileLength, code, err := client.getHTTPFileLength(url, headers)
	if err != nil {
		return -1, errors.Wrapf(dferrors.ErrUnknownError, "failed to get http file Length: %v", err)
	}

	if code == http.StatusUnauthorized || code == http.StatusProxyAuthRequired {
		return -1, errors.Wrapf(dferrors.ErrAuthenticationRequired, "url: %s,code: %d", url, code)
	}
	if code != http.StatusOK && code != http.StatusPartialContent {
		logrus.Warnf("failed to get http file length with unexpected code: %d", code)
		if code == http.StatusNotFound {
			return -1, errors.Wrapf(dferrors.ErrURLNotReachable, "url: %s", url)
		}
		return -1, nil
	}

	return fileLength, nil
}

// IsSupportRange checks if the source url support partial requests.
func (client *httpSourceClient) IsSupportRange(url string, headers map[string]string) (bool, error) {
	// set headers: headers is a reference to map, should not change it
	copied := CopyHeader(nil, headers)
	copied["Range"] = "bytes=0-0"

	// send request
	resp, err := client.httpWithHeaders(http.MethodGet, url, copied, 4*time.Second)
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusPartialContent {
		return true, nil
	}
	return false, nil
}

// IsExpired checks if a resource received or stored is the same.
func (client *httpSourceClient) IsExpired(url string, headers, expireInfo map[string]string) (bool, error) {
	lastModified, err := netutils.ConvertTimeStringToInt(expireInfo["Last-Modified"])
	if err != nil {
		return true, err
	}
	eTag := expireInfo["eTag"]
	if lastModified <= 0 && stringutils.IsEmptyStr(eTag) {
		return true, nil
	}

	// set headers: headers is a reference to map, should not change it
	copied := CopyHeader(nil, headers)
	if lastModified > 0 {
		copied["If-Modified-Since"] = expireInfo["Last-Modified"]
	}
	if !stringutils.IsEmptyStr(eTag) {
		copied["If-None-Match"] = eTag
	}

	// send request
	resp, err := client.httpWithHeaders(http.MethodGet, url, copied, 4*time.Second)
	if err != nil {
		return false, err
	}
	resp.Body.Close()

	return resp.StatusCode != http.StatusNotModified, nil
}

// Download downloads the file from the original address
func (client *httpSourceClient) Download(url string, headers map[string]string, checkCode StatusCodeChecker) (*types.DownloadResponse, error) {
	// TODO: add timeout
	resp, err := client.httpWithHeaders(http.MethodGet, url, headers, 0)
	if err != nil {
		return nil, err
	}

	if checkCode(resp.StatusCode) {
		return &types.DownloadResponse{
			Body: resp.Body,
			ExpireInfo: map[string]string{
				"Last-Modified": resp.Header.Get("Last-Modified"),
				"Etag":          resp.Header.Get("Etag"),
			},
		}, nil
	}
	return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// HTTPWithHeaders uses host-matched client to request the origin resource.
func (client *httpSourceClient) httpWithHeaders(method, url string, headers map[string]string, timeout time.Duration) (*http.Response, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	if timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		req = req.WithContext(ctx)
		defer cancel()
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	httpClientObject, existed := client.clientMap.Load(req.Host)
	if !existed {
		// use client.defaultHTTPClient to support custom protocols
		httpClientObject = client.defaultHTTPClient
	}

	httpClient, ok := httpClientObject.(*http.Client)
	if !ok {
		return nil, errors.Wrapf(dferrors.ErrInvalidValue, "http client type check error: %T", httpClientObject)
	}
	return httpClient.Do(req)
}

// CopyHeader copies the src to dst and return a non-nil dst map.
func CopyHeader(dst, src map[string]string) map[string]string {
	if dst == nil {
		dst = make(map[string]string)
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
