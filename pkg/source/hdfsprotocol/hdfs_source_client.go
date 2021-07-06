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

package hdfsprotocol

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net/url"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn"
	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"

	"d7y.io/dragonfly/v2/pkg/source"
	"github.com/colinmarc/hdfs/v2"
)

const (
	HDFSClient = "hdfs"
)
const (
	layout = "2006-01-02 15:04:05"
)

func init() {
	source.Register(HDFSClient, NewHDFSSourceClient())
}

// hdfsSourceClient is an implementation of the interface of SourceClient.
type hdfsSourceClient struct {
	sync.RWMutex
	clientMap map[string]*hdfs.Client
}

type HDFSSourceClientOption func(p *hdfsSourceClient)

func (h *hdfsSourceClient) GetContentLength(ctx context.Context, url string, header source.RequestHeader) (int64, error) {
	hdfsClient, path, err := h.getHDFSClientAndPath(url)
	if err != nil {
		return -1, err
	}
	info, err := hdfsClient.Stat(path)
	if err != nil {
		return -1, err
	}
	return info.Size(), nil
}

func (h *hdfsSourceClient) IsSupportRange(ctx context.Context, url string, header source.RequestHeader) (bool, error) {
	return true, nil
}

func (h *hdfsSourceClient) IsExpired(ctx context.Context, url string, header source.RequestHeader, expireInfo map[string]string) (bool, error) {
	lastModified := expireInfo[headers.LastModified]
	//eTag := expireInfo[headers.ETag]
	if lastModified == "" {
		return true, nil
	}

	hdfsClient, path, err := h.getHDFSClientAndPath(url)
	if err != nil {
		return false, err
	}

	info, err := hdfsClient.Stat(path)
	if err != nil {
		return false, err
	}
	t, err := time.ParseInLocation(layout, lastModified, time.Local)
	if err != nil {
		return false, err
	}
	return info.ModTime().Format(layout) != t.Format(layout), nil
}

func (h *hdfsSourceClient) Download(ctx context.Context, url string, header source.RequestHeader) (io.ReadCloser, error) {
	hdfsClient, path, err := h.getHDFSClientAndPath(url)
	if err != nil {
		return nil, err
	}
	hdfsFile, err := hdfsClient.Open(path)
	if err != nil {
		return nil, err
	}
	defer hdfsFile.Close()

	data, err := ioutil.ReadAll(hdfsFile)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bytes.NewBuffer(data)), nil
}

func (h *hdfsSourceClient) DownloadWithResponseHeader(ctx context.Context, url string, header source.RequestHeader) (io.ReadCloser, source.ResponseHeader, error) {

	breakPoint := header.Get(cdn.RangeHeaderName)

	if breakPoint == "" {
		// default start break 0
		breakPoint = "0"
	}

	breakPointLSize, err := strconv.ParseInt(breakPoint, 0, 64)
	if err != nil {
		return nil, nil, err
	}

	hdfsClient, path, err := h.getHDFSClientAndPath(url)
	if err != nil {
		return nil, nil, err
	}

	hdfsFile, err := hdfsClient.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer hdfsFile.Close()

	fileInfo := hdfsFile.Stat()

	if breakPointLSize > fileInfo.Size() {
		return nil, nil, errors.Errorf("hdfs Range more then file size,range start %d, file size %d", breakPointLSize, fileInfo.Size())
	}

	_, err = hdfsFile.Seek(breakPointLSize, 0)
	if err != nil {
		return nil, nil, err
	}

	expectReadSize := fileInfo.Size() - breakPointLSize
	data := make([]byte, expectReadSize)
	readLength, err := hdfsFile.Read(data)
	if err != nil || int64(readLength) != expectReadSize {
		return nil, nil, err
	}

	return ioutil.NopCloser(bytes.NewBuffer(data)), source.ResponseHeader{
		source.LastModified: fileInfo.ModTime().Format(layout),
	}, nil
}

func (h *hdfsSourceClient) GetLastModifiedMillis(ctx context.Context, url string, header source.RequestHeader) (int64, error) {

	hdfsClient, path, err := h.getHDFSClientAndPath(url)
	if err != nil {
		return -1, err
	}

	info, err := hdfsClient.Stat(path)
	if err != nil {
		return -1, err
	}

	return info.ModTime().UnixNano() / time.Millisecond.Nanoseconds(), nil
}

// getHDFSClient return hdfs client
func (h *hdfsSourceClient) getHDFSClient(rawurl string) (*hdfs.Client, error) {
	if len(rawurl) < 4 {
		return nil, errors.Errorf("hdfs url invalid: url is %s", rawurl)
	}

	parse, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	// get client for map
	h.RWMutex.RLock()
	if client, ok := h.clientMap[parse.Host]; ok {
		h.RWMutex.RUnlock()
		return client, nil
	}
	h.RWMutex.RUnlock()

	// create client option
	options := hdfs.ClientOptionsFromConf(map[string]string{
		"dfs.client.use.datanode.hostname": "true",
	})
	options.Addresses = strings.Split(parse.Host, ",")
	u, err := user.Current()
	if err != nil {
		return nil, err
	}
	options.User = u.Username

	// create hdfs client and put map
	h.RWMutex.Lock()
	client, err := hdfs.NewClient(options)
	if err != nil {
		h.RWMutex.Unlock()
		return nil, err
	}
	h.clientMap[parse.Host] = client
	h.RWMutex.Unlock()
	return client, err
}

// getHDFSPath return file path
func (h *hdfsSourceClient) getHDFSPath(urls string) (string, error) {
	parse, err := url.Parse(urls)
	if err != nil {
		return "", err
	}
	return parse.Path, nil
}

// getHDFSClientAndPath return client and path
func (h *hdfsSourceClient) getHDFSClientAndPath(urls string) (*hdfs.Client, string, error) {
	client, err := h.getHDFSClient(urls)
	if err != nil {
		return nil, "", errors.Errorf("hdfs create client fail, url is %s", urls)
	}
	path, err := h.getHDFSPath(urls)
	if err != nil {
		return client, "", errors.Errorf("hdfs url path parse fail, url is %s", urls)
	}
	return client, path, nil
}

func NewHDFSSourceClient(opts ...HDFSSourceClientOption) source.ResourceClient {
	sourceClient := &hdfsSourceClient{
		clientMap: make(map[string]*hdfs.Client),
	}
	for i := range opts {
		opts[i](sourceClient)
	}
	return sourceClient
}

var _ source.ResourceClient = (*hdfsSourceClient)(nil)
