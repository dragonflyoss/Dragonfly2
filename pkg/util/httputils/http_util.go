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

package httputils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/cdnerrors"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

/* http content types */
const (
	ApplicationJSONUtf8Value = "application/json;charset=utf-8"
)

var (
	// DefaultBuiltInTransport is the transport for HTTPWithHeaders.
	DefaultBuiltInTransport *http.Transport

	// DefaultBuiltInHTTPClient is the http client for HTTPWithHeaders.
	DefaultBuiltInHTTPClient *http.Client

	// DefaultHTTPClient is the default implementation of SimpleHTTPClient.
	DefaultHTTPClient SimpleHTTPClient = &defaultHTTPClient{}

	// validURLSchemas stores valid schemas
	// when call RegisterProtocol, validURLSchemas will be also updated.
	validURLSchemas = "https?|HTTPS?"
)

func init() {
	http.DefaultClient.Transport = &http.Transport{
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

	DefaultBuiltInTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	DefaultBuiltInHTTPClient = &http.Client{
		Transport: DefaultBuiltInTransport,
	}

	RegisterProtocolOnTransport(DefaultBuiltInTransport)
}

// SimpleHTTPClient defines some http functions used frequently.
type SimpleHTTPClient interface {

	// Get sends a GET request to server.
	// When timeout <= 0, it will block until receiving response from server.
	Get(url string, timeout time.Duration) (code int, res []byte, e error)

	// GetWithHeaders sends a GET request with headers to server.
	// When timeout <= 0, it will block until receiving response from server.
	GetWithHeaders(url string, headers map[string]string, timeout time.Duration) (code int, resBody []byte, err error)

	// PostJSON sends a POST request whose content-type is 'application/json;charset=utf-8' to server.
	// When timeout <= 0, it will block until receiving response from server.
	PostJSON(url string, body interface{}, timeout time.Duration) (code int, res []byte, e error)

	// PostJSONWithHeaders sends a POST request with headers whose content-type is 'application/json;charset=utf-8' to server.
	// When timeout <= 0, it will block until receiving response from server.
	PostJSONWithHeaders(url string, headers map[string]string, body interface{}, timeout time.Duration) (code int, resBody []byte, err error)
}

// ----------------------------------------------------------------------------
// defaultHTTPClient
type defaultHTTPClient struct {
}

var _ SimpleHTTPClient = &defaultHTTPClient{}

func (c *defaultHTTPClient) Get(url string, timeout time.Duration) (code int, body []byte, e error) {
	if timeout > 0 {
		return fasthttp.GetTimeout(nil, url, timeout)
	}
	return fasthttp.Get(nil, url)
}

func (c *defaultHTTPClient) GetWithHeaders(url string, headers map[string]string, timeout time.Duration) (
	code int, body []byte, e error) {
	return do(url, headers, timeout, nil)
}

func (c *defaultHTTPClient) PostJSON(url string, body interface{}, timeout time.Duration) (
	code int, resBody []byte, err error) {
	return c.PostJSONWithHeaders(url, nil, body, timeout)
}

func (c *defaultHTTPClient) PostJSONWithHeaders(url string, headers map[string]string, body interface{}, timeout time.Duration) (
	code int, resBody []byte, err error) {

	var jsonByte []byte

	if body != nil {
		jsonByte, err = json.Marshal(body)
		if err != nil {
			return fasthttp.StatusBadRequest, nil, err
		}
	}

	return do(url, headers, timeout, func(req *fasthttp.Request) error {
		req.SetBody(jsonByte)
		req.Header.SetMethod("POST")
		req.Header.SetContentType(ApplicationJSONUtf8Value)
		return nil
	})
}

// requestSetFunc a function that will set some values to the *req.
type requestSetFunc func(req *fasthttp.Request) error

func do(url string, headers map[string]string, timeout time.Duration, rsf requestSetFunc) (statusCode int, body []byte, err error) {
	// init request and response
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	// set request
	if rsf != nil {
		err = rsf(req)
		if err != nil {
			return
		}
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	// send request
	if timeout > 0 {
		err = fasthttp.DoTimeout(req, resp, timeout)
	} else {
		err = fasthttp.Do(req, resp)
	}
	if err != nil {
		return
	}

	statusCode = resp.StatusCode()
	data := resp.Body()
	body = make([]byte, len(data))
	copy(body, data)
	return
}

// ---------------------------------------------------------------------------
// util functions

// PostJSON sends a POST request whose content-type is 'application/json;charset=utf-8'.
func PostJSON(url string, body interface{}, timeout time.Duration) (int, []byte, error) {
	return DefaultHTTPClient.PostJSON(url, body, timeout)
}

// Get sends a GET request to server.
// When timeout <= 0, it will block until receiving response from server.
func Get(url string, timeout time.Duration) (int, []byte, error) {
	return DefaultHTTPClient.Get(url, timeout)
}

// PostJSONWithHeaders sends a POST request whose content-type is 'application/json;charset=utf-8'.
func PostJSONWithHeaders(url string, headers map[string]string, body interface{}, timeout time.Duration) (int, []byte, error) {
	return DefaultHTTPClient.PostJSONWithHeaders(url, headers, body, timeout)
}

// GetWithHeaders sends a GET request to server.
// When timeout <= 0, it will block until receiving response from server.
func GetWithHeaders(url string, headers map[string]string, timeout time.Duration) (code int, resBody []byte, err error) {
	return DefaultHTTPClient.GetWithHeaders(url, headers, timeout)
}

// Do performs the given http request and fills the given http response.
// When timeout <= 0, it will block until receiving response from server.
func Do(url string, headers map[string]string, timeout time.Duration) (string, error) {
	statusCode, body, err := do(url, headers, timeout, nil)
	if err != nil {
		return "", err
	}

	if statusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", statusCode)
	}

	result := string(body)

	return result, nil
}

// HTTPGet sends an HTTP GET request with headers.
func HTTPGet(url string, headers map[string]string) (*http.Response, error) {
	return HTTPWithHeaders("GET", url, headers, 0, nil)
}

// HTTPGetTimeout sends an HTTP GET request with timeout.
func HTTPGetTimeout(url string, headers map[string]string, timeout time.Duration) (*http.Response, error) {
	return HTTPWithHeaders("GET", url, headers, timeout, nil)
}

// HTTPGetWithTLS sends an HTTP GET request with TLS config.
func HTTPGetWithTLS(url string, headers map[string]string, timeout time.Duration, cacerts []string, insecure bool) (*http.Response, error) {
	roots := x509.NewCertPool()
	appendSuccess := false
	for _, certPath := range cacerts {
		certBytes, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, err
		}
		appendSuccess = appendSuccess || roots.AppendCertsFromPEM(certBytes)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecure,
	}
	if appendSuccess {
		tlsConfig.RootCAs = roots
	}
	return HTTPWithHeaders("GET", url, headers, timeout, tlsConfig)
}

// HTTPWithHeaders sends an HTTP request with headers and specified method.
func HTTPWithHeaders(method, url string, headers map[string]string, timeout time.Duration, tlsConfig *tls.Config) (*http.Response, error) {
	var (
		cancel func()
	)

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Add(k, v)
	}

	if timeout > 0 {
		timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		req = req.WithContext(timeoutCtx)
		cancel = cancelFunc
	}

	var c = DefaultBuiltInHTTPClient

	if tlsConfig != nil {
		// copy from http.DefaultTransport
		transport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		RegisterProtocolOnTransport(transport)
		transport.TLSClientConfig = tlsConfig

		c = &http.Client{
			Transport: transport,
		}
	}

	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}

	if cancel == nil {
		return res, nil
	}

	// do cancel() when close the body.
	res.Body = newWithFuncReadCloser(res.Body, cancel)
	return res, nil
}

func newWithFuncReadCloser(rc io.ReadCloser, f func()) io.ReadCloser {
	return &withFuncReadCloser{
		f:          f,
		ReadCloser: rc,
	}
}

type withFuncReadCloser struct {
	f func()
	io.ReadCloser
}

func (wrc *withFuncReadCloser) Close() error {
	if wrc.f != nil {
		wrc.f()
	}
	return wrc.ReadCloser.Close()
}

// protocols stores custom protocols
// key: schema
// value: transport
var protocols = sync.Map{}

// RegisterProtocol registers custom protocols in global variable "protocols"
// Example:
//   protocols := "helloworld"
//   newTransport := funcNewTransport
//   httputils.RegisterProtocol(protocols, newTransport)
// RegisterProtocol must be called before initialise dfget or cdnsystem instances.
func RegisterProtocol(scheme string, rt http.RoundTripper) {
	validURLSchemas += "|" + scheme
	protocols.Store(scheme, rt)
}

// RegisterProtocolOnTransport registers all new protocols in "protocols" for a special Transport
func RegisterProtocolOnTransport(tr *http.Transport) {
	protocols.Range(
		func(key, value interface{}) bool {
			tr.RegisterProtocol(key.(string), value.(http.RoundTripper))
			return true
		})
}

// ConstructRangeStr wraps the rangeStr as a HTTP Range header value.
func ConstructRangeStr(rangeStr string) string {
	return fmt.Sprintf("bytes=%s", rangeStr)
}

func GetValidURLSchemas() string {
	return validURLSchemas
}

// RangeStruct contains the start and end of a http header range.
type RangeStruct struct {
	StartIndex int64
	EndIndex   int64
}

// GetRange parses the start and the end from range HTTP header and returns them.
// rangeHTTPHeader bytes=0-1023,1024-1123
func GetRange(rangeHTTPHeader string, length int64) ([]*RangeStruct, error) {
	var rangeStr = rangeHTTPHeader

	// when rangeHTTPHeader looks like "bytes=0-1023", and then gets "0-1023".
	if strings.ContainsAny(rangeHTTPHeader, "=") {
		rangeSlice := strings.Split(rangeHTTPHeader, "=")
		if len(rangeSlice) != 2 {
			return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "invalid range: %s, should be like bytes=0-1023", rangeStr)
		}
		rangeStr = rangeSlice[1]
	}

	var result []*RangeStruct

	rangeArr := strings.Split(rangeStr, ",")
	rangeCount := len(rangeArr)
	if rangeCount == 0 {
		result = append(result, &RangeStruct{
			StartIndex: 0,
			EndIndex:   length - 1,
		})
		return result, nil
	}

	for i := 0; i < rangeCount; i++ {
		if strings.Count(rangeArr[i], "-") != 1 {
			return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "invalid range: %s, should be like 0-1023", rangeArr[i])
		}

		// -{length}
		if strings.HasPrefix(rangeArr[i], "-") {
			rangeStruct, err := handlePrefixRange(rangeArr[i], length)
			if err != nil {
				return nil, err
			}
			result = append(result, rangeStruct)
			continue
		}

		// {startIndex}-
		if strings.HasSuffix(rangeArr[i], "-") {
			rangeStruct, err := handleSuffixRange(rangeArr[i], length)
			if err != nil {
				return nil, err
			}
			result = append(result, rangeStruct)
			continue
		}

		rangeStruct, err := handlePairRange(rangeArr[i], length)
		if err != nil {
			return nil, err
		}
		result = append(result, rangeStruct)
	}
	return result, nil
}

func handlePrefixRange(rangeStr string, length int64) (*RangeStruct, error) {
	downLength, err := strconv.ParseInt(strings.TrimPrefix(rangeStr, "-"), 10, 64)
	if err != nil || downLength < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}

	if downLength > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	return &RangeStruct{
		StartIndex: length - downLength,
		EndIndex:   length - 1,
	}, nil
}

func handleSuffixRange(rangeStr string, length int64) (*RangeStruct, error) {
	startIndex, err := strconv.ParseInt(strings.TrimSuffix(rangeStr, "-"), 10, 64)
	if err != nil || startIndex < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}

	if startIndex > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	return &RangeStruct{
		StartIndex: startIndex,
		EndIndex:   length - 1,
	}, nil
}

func handlePairRange(rangeStr string, length int64) (*RangeStruct, error) {
	rangePair := strings.Split(rangeStr, "-")

	startIndex, err := strconv.ParseInt(rangePair[0], 10, 64)
	if err != nil || startIndex < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}
	if startIndex > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	endIndex, err := strconv.ParseInt(rangePair[1], 10, 64)
	if err != nil || endIndex < 0 {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "failed to parse range: %s to int: %v", rangeStr, err)
	}
	if endIndex > length {
		return nil, errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "range: %s", rangeStr)
	}

	if endIndex < startIndex {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "range: %s, the start is larger the end", rangeStr)
	}

	return &RangeStruct{
		StartIndex: startIndex,
		EndIndex:   endIndex,
	}, nil
}
