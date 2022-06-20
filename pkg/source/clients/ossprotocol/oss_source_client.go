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
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/strings"
)

const OSSClient = "oss"

const (
	endpoint        = "endpoint"
	accessKeyID     = "accessKeyID"
	accessKeySecret = "accessKeySecret"
)

var _ source.ResourceClient = (*ossSourceClient)(nil)

func init() {
	if err := source.Register(OSSClient, NewOSSSourceClient(), adaptor); err != nil {
		panic(err)
	}
}

func adaptor(request *source.Request) *source.Request {
	clonedRequest := request.Clone(request.Context())
	if request.Header.Get(source.Range) != "" {
		clonedRequest.Header.Set(oss.HTTPHeaderRange, fmt.Sprintf("bytes=%s", request.Header.Get(source.Range)))
		clonedRequest.Header.Del(source.Range)
	}
	if request.Header.Get(source.LastModified) != "" {
		clonedRequest.Header.Set(oss.HTTPHeaderLastModified, request.Header.Get(source.LastModified))
		clonedRequest.Header.Del(source.LastModified)
	}
	if request.Header.Get(source.ETag) != "" {
		clonedRequest.Header.Set(oss.HTTPHeaderEtag, request.Header.Get(source.ETag))
		clonedRequest.Header.Del(source.ETag)
	}
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
		return source.UnknownSourceFileLen, errors.Wrapf(err, "get oss bucket: %s", request.URL.Host)
	}
	header, err := bucket.GetObjectMeta(request.URL.Path, getOptions(request.Header)...)
	if err != nil {
		return source.UnknownSourceFileLen, errors.Wrapf(err, "get oss object %s meta", request.URL.Path)
	}
	contentLen, err := strconv.ParseInt(header.Get(oss.HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return source.UnknownSourceFileLen, errors.Wrapf(err, "parse content-length str to int64")
	}
	return contentLen, nil
}

func (osc *ossSourceClient) IsSupportRange(request *source.Request) (bool, error) {
	if request.Header.Get(oss.HTTPHeaderRange) == "" {
		request.Header.Set(oss.HTTPHeaderRange, "bytes=0-0")
	}
	client, err := osc.getClient(request.Header)
	if err != nil {
		return false, errors.Wrap(err, "get oss client")
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return false, errors.Wrapf(err, "get oss bucket: %s", request.URL.Host)
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

func (osc *ossSourceClient) IsExpired(request *source.Request, info *source.ExpireInfo) (bool, error) {
	client, err := osc.getClient(request.Header)
	if err != nil {
		return false, errors.Wrap(err, "get oss client")
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return false, errors.Wrapf(err, "get oss bucket: %s", request.URL.Host)
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
		return nil, errors.Wrapf(err, "get oss client")
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return nil, errors.Wrapf(err, "get oss bucket: %s", request.URL.Host)
	}
	objectResult, err := bucket.DoGetObject(&oss.GetObjectRequest{ObjectKey: request.URL.Path}, getOptions(request.Header))
	if err != nil {
		return nil, errors.Wrapf(err, "get oss Object: %s", request.URL.Path)
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
	return response, nil
}

func (osc *ossSourceClient) GetLastModified(request *source.Request) (int64, error) {
	client, err := osc.getClient(request.Header)
	if err != nil {
		return -1, errors.Wrap(err, "get oss client")
	}
	bucket, err := client.Bucket(request.URL.Host)
	if err != nil {
		return -1, errors.Wrapf(err, "get oss bucket: %s", request.URL.Host)
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

func (osc *ossSourceClient) getClient(header source.Header) (*oss.Client, error) {
	endpoint := header.Get(endpoint)
	if strings.IsBlank(endpoint) {
		return nil, errors.New("endpoint is empty")
	}
	accessKeyID := header.Get(accessKeyID)
	if strings.IsBlank(accessKeyID) {
		return nil, errors.New("accessKeyID is empty")
	}
	accessKeySecret := header.Get(accessKeySecret)
	if strings.IsBlank(accessKeySecret) {
		return nil, errors.New("accessKeySecret is empty")
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
	for key, value := range header {
		if key == endpoint || key == accessKeyID || key == accessKeySecret {
			continue
		}
		opts = append(opts, oss.SetHeader(key, value))
	}
	return opts
}
