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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"
)

const ossClient = "oss"

const (
	endpoint        = "endpoint"
	accessKeyID     = "accessKeyID"
	accessKeySecret = "accessKeySecret"
)

var _ source.ResourceClient = (*ossSourceClient)(nil)

func init() {
	sourceClient := NewOSSSourceClient()
	source.Register(ossClient, sourceClient)
}

func NewOSSSourceClient(opts ...OssSourceClientOption) source.ResourceClient {
	sourceClient := &ossSourceClient{
		clientMap: sync.Map{},
		accessMap: sync.Map{},
	}
	for i := range opts {
		opts[i](sourceClient)
	}
	return sourceClient
}

type OssSourceClientOption func(p *ossSourceClient)

// ossSourceClient is an implementation of the interface of SourceClient.
type ossSourceClient struct {
	// endpoint_accessKeyID_accessKeySecret -> ossClient
	clientMap sync.Map
	accessMap sync.Map
}

func (osc *ossSourceClient) Download(ctx context.Context, url string, header source.RequestHeader) (io.ReadCloser, error) {
	panic("implement me")
}

func (osc *ossSourceClient) GetLastModifiedMillis(ctx context.Context, url string, header source.RequestHeader) (int64, error) {
	panic("implement me")
}

func (osc *ossSourceClient) GetContentLength(ctx context.Context, url string, header source.RequestHeader) (int64, error) {
	resHeader, err := osc.getMeta(ctx, url, header)
	if err != nil {
		return -1, err
	}

	contentLen, err := strconv.ParseInt(resHeader.Get(oss.HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return -1, err
	}

	return contentLen, nil
}

func (osc *ossSourceClient) IsSupportRange(ctx context.Context, url string, header source.RequestHeader) (bool, error) {
	_, err := osc.getMeta(ctx, url, header)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (osc *ossSourceClient) IsExpired(ctx context.Context, url string, header source.RequestHeader, expireInfo map[string]string) (bool, error) {
	lastModified := expireInfo[oss.HTTPHeaderLastModified]
	eTag := expireInfo[oss.HTTPHeaderEtag]
	if stringutils.IsBlank(lastModified) && stringutils.IsBlank(eTag) {
		return true, nil
	}

	resHeader, err := osc.getMeta(ctx, url, header)
	if err != nil {
		return false, err
	}
	return resHeader.Get(oss.HTTPHeaderLastModified) == expireInfo[oss.HTTPHeaderLastModified] && resHeader.Get(oss.HTTPHeaderEtag) == expireInfo[oss.
		HTTPHeaderEtag], nil
}

func (osc *ossSourceClient) DownloadWithResponseHeader(ctx context.Context, url string, header source.RequestHeader) (io.ReadCloser, source.ResponseHeader, error) {
	ossObject, err := parseOssObject(url)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "parse oss object from url:%s", url)
	}
	client, err := osc.getClient(header)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get client")
	}
	bucket, err := client.Bucket(ossObject.bucket)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get bucket:%s", ossObject.bucket)
	}
	res, err := bucket.GetObject(ossObject.object, getOptions(header)...)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get oss Object:%s", ossObject.object)
	}
	resp := res.(*oss.Response)
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
		responseHeader := source.ResponseHeader{
			source.LastModified: resp.Headers.Get(headers.LastModified),
			source.ETag:         resp.Headers.Get(headers.ETag),
		}
		return resp.Body, responseHeader, nil
	}
	resp.Body.Close()
	return nil, nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

func (osc *ossSourceClient) getClient(header map[string]string) (*oss.Client, error) {
	endpoint, ok := header[endpoint]
	if !ok {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "endpoint is empty")
	}
	accessKeyID, ok := header[accessKeyID]
	if !ok {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "accessKeyID is empty")
	}
	accessKeySecret, ok := header[accessKeySecret]
	if !ok {
		return nil, errors.Wrapf(cdnerrors.ErrInvalidValue, "accessKeySecret is empty")
	}
	clientKey := genClientKey(endpoint, accessKeyID, accessKeySecret)
	if client, ok := osc.clientMap.Load(clientKey); ok {
		return client.(*oss.Client), nil
	}
	client, err := oss.New(endpoint, accessKeyID, accessKeySecret)
	if err != nil {
		return nil, err
	}
	osc.clientMap.Store(clientKey, client)
	return client, nil
}

func genClientKey(endpoint, accessKeyID, accessKeySecret string) string {
	return fmt.Sprintf("%s_%s_%s", endpoint, accessKeyID, accessKeySecret)
}

func (osc *ossSourceClient) getMeta(ctx context.Context, url string, header map[string]string) (http.Header, error) {
	client, err := osc.getClient(header)
	if err != nil {
		return nil, errors.Wrapf(err, "get oss client")
	}
	ossObject, err := parseOssObject(url)
	if err != nil {
		return nil, errors.Wrapf(err, "parse oss object")
	}

	bucket, err := client.Bucket(ossObject.bucket)
	if err != nil {
		return nil, errors.Wrapf(err, "get bucket:%s", ossObject.bucket)
	}
	isExist, err := bucket.IsObjectExist(ossObject.object)
	if err != nil {
		return nil, errors.Wrapf(err, "prob object:%s if exist", ossObject.object)
	}
	if !isExist {
		return nil, fmt.Errorf("oss object:%s does not exist", ossObject.object)
	}
	return bucket.GetObjectMeta(ossObject.object, getOptions(header)...)
}

func getOptions(header map[string]string) []oss.Option {
	opts := make([]oss.Option, 0, len(header))
	for key, value := range header {
		if key == endpoint || key == accessKeyID || key == accessKeySecret {
			continue
		}
		opts = append(opts, oss.SetHeader(key, value))
	}
	return opts
}

type ossObject struct {
	endpoint string
	bucket   string
	object   string
}

func parseOssObject(rawURL string) (*ossObject, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.Wrapf(err, "parse rawURL: %s failed", rawURL)
	}
	if parsedURL.Scheme != "oss" {
		return nil, fmt.Errorf("rawUrl:%s is not oss url", rawURL)
	}
	return &ossObject{
		endpoint: parsedURL.Path[0:2],
		bucket:   parsedURL.Host,
		object:   parsedURL.Path[1:],
	}, nil
}
