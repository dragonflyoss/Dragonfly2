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

package ossprotocol

import (
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-http-utils/headers"

	"d7y.io/dragonfly/v2/pkg/source"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

const OSSClient = "oss"

const (
	endpoint        = "endpoint"
	accessKeyID     = "accessKeyID"
	accessKeySecret = "accessKeySecret"
	securityToken   = "securityToken"
)

var _ source.ResourceClient = (*ossSourceClient)(nil)

func init() {
	source.RegisterBuilder(OSSClient, source.NewPlainResourceClientBuilder(Builder))
}

func Builder(optionYaml []byte) (source.ResourceClient, source.RequestAdapter, []source.Hook, error) {
	return NewOSSSourceClient(), adaptor, nil, nil
}

func adaptor(request *source.Request) *source.Request {
	clonedRequest := request.Clone(request.Context())
	if request.Header.Get(source.Range) != "" {
		clonedRequest.Header.Set(oss.HTTPHeaderRange, fmt.Sprintf("bytes=%s", request.Header.Get(source.Range)))
		clonedRequest.Header.Del(source.Range)
	}
	clonedRequest.URL.Path = strings.TrimPrefix(clonedRequest.URL.Path, "/")
	return clonedRequest
}

func NewOSSSourceClient(opts ...OSSSourceClientOption) source.ResourceClient {
	return newOSSSourceClient(opts...)
}

func newOSSSourceClient(opts ...OSSSourceClientOption) source.ResourceClient {
	sourceClient := &ossSourceClient{
		clientMap: sync.Map{},
		accessMap: sync.Map{},
	}
	for i := range opts {
		opts[i](sourceClient)
	}
	return sourceClient
}

type OSSSourceClientOption func(p *ossSourceClient)

// ossSourceClient is an implementation of the interface of source.ResourceClient.
type ossSourceClient struct {
	// endpoint_accessKeyID_accessKeySecret -> ossClient
	clientMap sync.Map
	accessMap sync.Map
}

func (osc *ossSourceClient) GetContentLength(request *source.Request) (int64, error) {
	client, err := osc.getClient(request.Header)
	if err != nil {
		return source.UnknownSourceFileLen, err
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return source.UnknownSourceFileLen, fmt.Errorf("get oss bucket %s: %w", request.URL.Host, err)
	}
	header, err := bucket.GetObjectMeta(request.URL.Path, getOptions(request.Header)...)
	if err != nil {
		return source.UnknownSourceFileLen, fmt.Errorf("get oss object %s meta: %w", request.URL.Path, err)
	}
	contentLen, err := strconv.ParseInt(header.Get(oss.HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return source.UnknownSourceFileLen, fmt.Errorf("parse content-length str to int64: %w", err)
	}
	return contentLen, nil
}

func (osc *ossSourceClient) IsSupportRange(request *source.Request) (bool, error) {
	if request.Header.Get(oss.HTTPHeaderRange) == "" {
		request.Header.Set(oss.HTTPHeaderRange, "bytes=0-0")
	}
	client, err := osc.getClient(request.Header)
	if err != nil {
		return false, fmt.Errorf("get oss client: %w", err)
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return false, fmt.Errorf("get oss bucket %s: %w", request.URL.Host, err)
	}
	exist, err := bucket.IsObjectExist(request.URL.Path, getOptions(request.Header)...)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, source.ErrResourceNotReachable
	}
	return true, nil
}

func (osc *ossSourceClient) GetMetadata(request *source.Request) (*source.Metadata, error) {
	request = request.Clone(request.Context())
	request.Header.Set(headers.Range, "bytes=0-0")

	client, err := osc.getClient(request.Header)
	if err != nil {
		return nil, fmt.Errorf("get oss client: %w", err)
	}

	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("get oss bucket %s: %w", request.URL.Host, err)
	}

	header, err := bucket.GetObjectMeta(request.URL.Path, getOptions(request.Header)...)
	if err != nil {
		return nil, fmt.Errorf("get oss object %s meta: %w", request.URL.Path, err)
	}
	totalContentLength, err := strconv.ParseInt(header.Get(oss.HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse content-length str to int64: %w", err)
	}

	hdr := source.Header{}
	for k, v := range header {
		if len(v) > 0 {
			hdr.Set(k, v[0])
		}
	}
	return &source.Metadata{
		Header:             hdr,
		Status:             http.StatusText(http.StatusOK),
		StatusCode:         http.StatusOK,
		SupportRange:       true,
		TotalContentLength: totalContentLength,
		Validate: func() error {
			return nil
		},
		Temporary: true,
	}, nil
}

func (osc *ossSourceClient) IsExpired(request *source.Request, info *source.ExpireInfo) (bool, error) {
	client, err := osc.getClient(request.Header)
	if err != nil {
		return false, fmt.Errorf("get oss client: %w", err)
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return false, fmt.Errorf("get oss bucket %s: %w", request.URL.Host, err)
	}
	resHeader, err := bucket.GetObjectMeta(request.URL.Path, getOptions(request.Header)...)
	if err != nil {
		return false, err
	}
	return !(resHeader.Get(oss.HTTPHeaderEtag) == info.ETag || resHeader.Get(oss.HTTPHeaderLastModified) == info.LastModified), nil
}

func (osc *ossSourceClient) Download(request *source.Request) (*source.Response, error) {
	client, err := osc.getClient(request.Header)
	if err != nil {
		return nil, fmt.Errorf("get oss client: %w", err)
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("get oss bucket %s: %w", request.URL.Host, err)
	}
	objectResult, err := bucket.DoGetObject(
		&oss.GetObjectRequest{ObjectKey: request.URL.Path},
		getOptions(request.Header),
	)
	if err != nil {
		return nil, fmt.Errorf("get oss Object %s: %w", request.URL.Path, err)
	}
	err = source.CheckResponseCode(objectResult.Response.StatusCode, []int{http.StatusOK, http.StatusPartialContent})
	if err != nil {
		objectResult.Response.Body.Close()
		return nil, err
	}
	response := source.NewResponse(
		objectResult.Response.Body,
		source.WithExpireInfo(
			source.ExpireInfo{
				LastModified: objectResult.Response.Headers.Get(headers.LastModified),
				ETag:         objectResult.Response.Headers.Get(headers.ETag),
			},
		))
	contentLength := objectResult.Response.Headers.Get(oss.HTTPHeaderContentLength)
	if !pkgstrings.IsBlank(contentLength) {
		if len, err := strconv.ParseInt(contentLength, 10, 64); err == nil {
			response.ContentLength = len
		}
	}
	return response, nil
}

func (osc *ossSourceClient) GetLastModified(request *source.Request) (int64, error) {
	client, err := osc.getClient(request.Header)
	if err != nil {
		return -1, fmt.Errorf("get oss client: %w", err)
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return -1, fmt.Errorf("get oss bucket %s: %w", request.URL.Host, err)
	}
	header, err := bucket.GetObjectMeta(request.URL.Path, getOptions(request.Header)...)
	if err != nil {
		return -1, err
	}

	lastModified := header.Get(oss.HTTPHeaderLastModified)
	if lastModified == "" {
		return -1, err
	}

	t, err := time.ParseInLocation(source.TimeFormat, lastModified, time.UTC)
	if err != nil {
		return -1, err
	}

	return t.UnixNano() / time.Millisecond.Nanoseconds(), nil
}

func (osc *ossSourceClient) List(request *source.Request) (urls []source.URLEntry, err error) {
	client, err := osc.getClient(request.Header)
	if err != nil {
		return nil, fmt.Errorf("get oss client: %w", err)
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return nil, fmt.Errorf("get oss bucket %s: %w", request.URL.Host, err)
	}
	isDir, err := osc.isDirectory(bucket, request)
	if err != nil {
		return nil, err
	}
	// if request is a single file, just return
	if !isDir {
		return []source.URLEntry{buildURLEntry(false, request.URL)}, nil
	}
	// list all files and subdirectory
	path := addTrailingSlash(request.URL.Path)
	prefix := oss.Prefix(path)
	marker := oss.Marker("")
	delimiter := "/"
	for {
		lsRes, err := bucket.ListObjects(prefix, marker, oss.Delimiter(delimiter))
		if err != nil {
			return urls, fmt.Errorf("list oss object %s/%s: %w", request.URL.Host, path, err)
		}
		for _, object := range lsRes.Objects {
			if object.Key != lsRes.Prefix {
				url := *request.URL
				url.Path = addLeadingSlash(object.Key)
				urls = append(urls, buildURLEntry(false, &url))
			}
		}
		for _, object := range lsRes.CommonPrefixes {
			url := *request.URL
			url.Path = addLeadingSlash(object)
			urls = append(urls, buildURLEntry(true, &url))
		}
		if lsRes.IsTruncated {
			prefix = oss.Prefix(lsRes.Prefix)
			marker = oss.Marker(lsRes.NextMarker)
		} else {
			break
		}
	}
	return urls, nil
}

func (osc *ossSourceClient) isDirectory(bucket *oss.Bucket, request *source.Request) (bool, error) {
	uPath := addTrailingSlash(request.URL.Path)
	lsRes, err := bucket.ListObjects(oss.Prefix(uPath), oss.Marker(""), oss.MaxKeys(1))
	if err != nil {
		return false, fmt.Errorf("list oss object %s/%s: %w", request.URL.Host, uPath, err)
	}
	if len(lsRes.Objects)+len(lsRes.CommonPrefixes) > 0 {
		return true, nil
	}
	return false, nil
}

func (osc *ossSourceClient) getClient(header source.Header) (*oss.Client, error) {
	endpoint := header.Get(endpoint)
	if pkgstrings.IsBlank(endpoint) {
		return nil, errors.New("endpoint is empty")
	}
	accessKeyID := header.Get(accessKeyID)
	if pkgstrings.IsBlank(accessKeyID) {
		return nil, errors.New("accessKeyID is empty")
	}
	accessKeySecret := header.Get(accessKeySecret)
	if pkgstrings.IsBlank(accessKeySecret) {
		return nil, errors.New("accessKeySecret is empty")
	}
	securityToken := header.Get(securityToken)
	if !pkgstrings.IsBlank(securityToken) {
		return oss.New(endpoint, accessKeyID, accessKeySecret, oss.SecurityToken(securityToken))
	}
	clientKey := buildClientKey(endpoint, accessKeyID, accessKeySecret)
	if client, ok := osc.clientMap.Load(clientKey); ok {
		return client.(*oss.Client), nil
	}

	client, err := oss.New(endpoint, accessKeyID, accessKeySecret)

	if err != nil {
		return nil, err
	}
	actual, _ := osc.clientMap.LoadOrStore(clientKey, client)
	return actual.(*oss.Client), nil
}

func buildClientKey(endpoint, accessKeyID, accessKeySecret string) string {
	return fmt.Sprintf("%s_%s_%s", endpoint, accessKeyID, accessKeySecret)
}

func getOptions(header source.Header) []oss.Option {
	opts := make([]oss.Option, 0, len(header))
	for key, values := range header {
		if key == textproto.CanonicalMIMEHeaderKey(endpoint) || key == textproto.CanonicalMIMEHeaderKey(accessKeyID) || key == textproto.CanonicalMIMEHeaderKey(accessKeySecret) {
			continue
		}
		// oss-http Header value must be string type, while http header value is []string type
		// according to HTTP RFC2616 Multiple message-header fields with the same field-name MAY be present in a message
		// if and only if the entire field-value for that header field is defined as a comma-separated list
		value := strings.Join(values, ",")
		opts = append(opts, oss.SetHeader(key, value))
	}
	return opts
}

func buildURLEntry(isDir bool, url *url.URL) source.URLEntry {
	if isDir {
		url.Path = addTrailingSlash(url.Path)
		list := strings.Split(url.Path, "/")
		return source.URLEntry{URL: url, Name: list[len(list)-2], IsDir: true}
	}
	_, name := filepath.Split(url.Path)
	return source.URLEntry{URL: url, Name: name, IsDir: false}
}

func addLeadingSlash(s string) string {
	if strings.HasPrefix(s, "/") {
		return s
	}
	return "/" + s
}

func addTrailingSlash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}
