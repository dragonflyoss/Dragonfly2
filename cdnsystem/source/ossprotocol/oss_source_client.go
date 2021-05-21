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
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/source"
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

func init() {
	sourceClient := NewOSSSourceClient()
	source.Register(ossClient, sourceClient)
}

func NewOSSSourceClient() source.ResourceClient {
	return &ossSourceClient{
		clientMap: sync.Map{},
		accessMap: sync.Map{},
	}
}

// httpSourceClient is an implementation of the interface of SourceClient.
type ossSourceClient struct {
	// endpoint -> ossClient
	clientMap sync.Map
	accessMap sync.Map
}

func (osc *ossSourceClient) GetContentLength(url string, header map[string]string) (int64, error) {
	resHeader, err := osc.getMeta(url, header)
	if err != nil {
		return -1, err
	}

	contentLen, err := strconv.ParseInt(resHeader.Get(oss.HTTPHeaderContentLength), 10, 64)
	if err != nil {
		return -1, err
	}

	return contentLen, nil
}

func (osc *ossSourceClient) IsSupportRange(url string, header map[string]string) (bool, error) {
	_, err := osc.getMeta(url, header)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (osc *ossSourceClient) IsExpired(url string, header, expireInfo map[string]string) (bool, error) {
	lastModified := expireInfo[oss.HTTPHeaderLastModified]
	eTag := expireInfo[oss.HTTPHeaderEtag]
	if stringutils.IsBlank(lastModified) && stringutils.IsBlank(eTag) {
		return true, nil
	}

	resHeader, err := osc.getMeta(url, header)
	if err != nil {
		return false, err
	}
	return resHeader.Get(oss.HTTPHeaderLastModified) == expireInfo[oss.HTTPHeaderLastModified] && resHeader.Get(oss.HTTPHeaderEtag) == expireInfo[oss.
		HTTPHeaderEtag], nil
}

func (osc *ossSourceClient) Download(url string, header map[string]string) (io.ReadCloser, map[string]string, error) {
	ossObject, err := ParseOssObject(url)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to parse oss object from url:%s", url)
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
		expireInfo := map[string]string{
			headers.LastModified: resp.Headers.Get(headers.LastModified),
			headers.ETag:         resp.Headers.Get(headers.ETag),
		}
		return resp.Body, expireInfo, nil
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
	if client, ok := osc.clientMap.Load(endpoint); ok {
		return client.(*oss.Client), nil
	}
	client, err := oss.New(endpoint, accessKeyID, accessKeySecret)
	if err != nil {
		return nil, err
	}
	osc.clientMap.Store(endpoint, client)
	return client, nil
}

func (osc *ossSourceClient) getMeta(url string, header map[string]string) (http.Header, error) {
	client, err := osc.getClient(header)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get oss client")
	}
	ossObject, err := ParseOssObject(url)
	if err != nil {
		return nil, errors.Wrapf(cdnerrors.ErrURLNotReachable, "failed to parse oss object: %v", err)
	}

	bucket, err := client.Bucket(ossObject.bucket)
	if err != nil {
		return nil, errors.Wrapf(cdnerrors.ErrURLNotReachable, "failed to get bucket:%s: %v", ossObject.bucket, err)
	}
	isExist, err := bucket.IsObjectExist(ossObject.object)
	if err != nil {
		return nil, errors.Wrapf(cdnerrors.ErrURLNotReachable, "failed to prob object:%s exist: %v", ossObject.object, err)
	}
	if !isExist {
		return nil, errors.Wrapf(cdnerrors.ErrURLNotReachable, "oss object:%s does not exist", ossObject.object)
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
	bucket string
	object string
}

func ParseOssObject(ossUrl string) (*ossObject, error) {
	parsedUrl, err := url.Parse(ossUrl)
	if parsedUrl.Scheme != "oss" {
		return nil, fmt.Errorf("url:%s is not oss object", ossUrl)
	}
	if err != nil {
		return nil, err
	}
	return &ossObject{
		bucket: parsedUrl.Host,
		object: parsedUrl.Path[1:],
	}, nil
}
