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
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/source"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/structure/maputils"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"d7y.io/dragonfly/v2/pkg/util/timeutils"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/pkg/errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
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
		//return , err
	}
	contentLen, err := strconv.ParseInt(resHeader.Get(oss.HTTPHeaderContentLength), 10, 64)
	return contentLen, nil
}

func (osc *ossSourceClient) getMeta(url string, header map[string]string) (http.Header, error) {
	client, err := osc.getClient(header)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get oss client")
	}
	ossObject, err := ParseOssObject(url)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse oss object")
	}

	bucket, err := client.Bucket(ossObject.bucket)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get bucket:%s", ossObject.bucket)
	}
	isExist, err := bucket.IsObjectExist(ossObject.object)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to prob object:%s exist", ossObject.object)
	}
	if !isExist {
		return nil, fmt.Errorf("oss object:%s does not exist", ossObject.object)
	}
	return bucket.GetObjectMeta(ossObject.object, getOptions(header)...)
}

func (osc *ossSourceClient) IsSupportRange(url string, header map[string]string) (bool, error) {
	return true, nil
}

func (osc *ossSourceClient) IsExpired(url string, header, expireInfo map[string]string) (bool, error) {

	lastModified := timeutils.UnixMillis(expireInfo[oss.HTTPHeaderLastModified])

	eTag := expireInfo[oss.HTTPHeaderEtag]
	if lastModified <= 0 && stringutils.IsBlank(eTag) {
		return true, nil
	}

	// set header: header is a reference to map, should not change it
	copied := maputils.DeepCopyMap(nil, header)
	if lastModified > 0 {
		copied[oss.HTTPHeaderIfModifiedSince] = expireInfo[oss.HTTPHeaderLastModified]
	}
	if !stringutils.IsBlank(eTag) {
		copied[oss.HTTPHeaderIfNoneMatch] = eTag
	}
	return true, nil
	//// send request
	//resp, err := osc.clientMap.httpWithHeader(http.MethodGet, url, copied, 4*time.Second)
	//if err != nil {
	//	return false, err
	//}
	//resp.Body.Close()
	//
	//return resp.StatusCode != http.StatusNotModified, nil
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
	body, err := bucket.GetObject(ossObject.object, getOptions(header)...)
	if err != nil {
		logger.Errorf("Cannot get the specified bucket %s instance: %v", bucket, err)
		os.Exit(1)
	}
	//ExpireInfo:
	//map[string]string{
	//	"Last-Modified": resp.Header.Get("Last-Modified"),
	//	"Etag":          resp.Header.Get("Etag"),
	//},
	//	resp.Body.Close()
	//return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	return body, nil, nil
}

func (osc *ossSourceClient) getClient(header map[string]string) (*oss.Client, error) {
	endpoint, ok := header[endpoint]
	if !ok {
		return nil, errors.Wrapf(cdnerrors.ErrEmptyValue, "endpoint is empty")
	}
	accessKeyID, ok := header[accessKeyID]
	if !ok {
		return nil, errors.Wrapf(cdnerrors.ErrEmptyValue, "accessKeyID is empty")
	}
	accessKeySecret, ok := header[accessKeySecret]
	if !ok {
		return nil, errors.Wrapf(cdnerrors.ErrEmptyValue, "accessKeySecret is empty")
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
