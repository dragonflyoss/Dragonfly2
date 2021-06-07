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
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"
	"github.com/go-http-utils/headers"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/suite"
)

func TestHTTPSourceClientTestSuite(t *testing.T) {
	suite.Run(t, new(HTTPSourceClientTestSuite))
}

type HTTPSourceClientTestSuite struct {
	suite.Suite
	source.ResourceClient
}

func (suite *HTTPSourceClientTestSuite) SetupSuite() {
	suite.ResourceClient = NewHTTPSourceClient()
	httpmock.ActivateNonDefault(defaultHTTPClient)
}

func (suite *HTTPSourceClientTestSuite) TearDownSuite() {
	httpmock.DeactivateAndReset()
}

var (
	timeoutURL               = "http://timeout.com"
	normalURL                = "http://normal.com"
	errorURL                 = "http://error.com"
	forbiddenURL             = "http://forbidden.com"
	notfoundURL              = "http://notfound.com"
	normalNotSupportRangeURL = "http://notsuppertrange.com"
)

var (
	testContent  = "l am test case"
	lastModified = "Sun, 06 Jun 2021 12:52:30 GMT"
	//etag         = "UMiJT4h7MCEAEgnqCLA2CdAaABnK" // todo etag business code can not obtain
	etag = ""
)

func (suite *HTTPSourceClientTestSuite) SetupTest() {
	httpmock.Reset()
	httpmock.RegisterResponder(http.MethodGet, timeoutURL, func(request *http.Request) (*http.Response, error) {
		// To simulate the timeout
		time.Sleep(5 * time.Second)
		return httpmock.NewStringResponse(http.StatusOK, "ok"), nil
	})

	httpmock.RegisterResponder(http.MethodGet, normalURL, func(request *http.Request) (*http.Response, error) {
		if rang := request.Header.Get(headers.Range); rang != "" {
			r, _ := rangeutils.ParseRange(rang[6:])
			res := &http.Response{
				StatusCode:    http.StatusPartialContent,
				ContentLength: int64(r.EndIndex) - int64(r.StartIndex) + int64(1),
				Body:          httpmock.NewRespBodyFromString(testContent[r.StartIndex:r.EndIndex]),
				Header: http.Header{
					headers.LastModified: []string{lastModified},
					headers.ETag:         []string{etag},
				},
			}
			return res, nil
		}
		if expire := request.Header.Get(headers.IfModifiedSince); expire != "" {
			res := &http.Response{
				StatusCode:    http.StatusNotModified,
				ContentLength: int64(len(testContent)),
				Body:          httpmock.NewRespBodyFromString(testContent),
				Header: http.Header{
					headers.LastModified: []string{lastModified},
					headers.ETag:         []string{etag},
				},
			}
			return res, nil
		}
		res := &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: 14,
			Body:          httpmock.NewRespBodyFromString(testContent),
			Header: http.Header{
				headers.LastModified: []string{lastModified},
				headers.ETag:         []string{etag},
			},
		}
		return res, nil
	})

	httpmock.RegisterResponder(http.MethodGet, forbiddenURL, httpmock.NewStringResponder(http.StatusForbidden, "forbidden"))
	httpmock.RegisterResponder(http.MethodGet, notfoundURL, httpmock.NewStringResponder(http.StatusNotFound, "not found"))
	httpmock.RegisterResponder(http.MethodGet, normalNotSupportRangeURL, httpmock.NewStringResponder(http.StatusOK, testContent))
	httpmock.RegisterResponder(http.MethodGet, errorURL, httpmock.NewErrorResponder(fmt.Errorf("error")))
}

func (suite *HTTPSourceClientTestSuite) TestNewHTTPSourceClient() {
	var sourceClient source.ResourceClient
	sourceClient = NewHTTPSourceClient()
	suite.Equal(defaultHTTPClient, sourceClient.(*httpSourceClient).httpClient)
	suite.EqualValues(*defaultHTTPClient, *sourceClient.(*httpSourceClient).httpClient)

	expectedHTTPClient := &http.Client{}
	sourceClient = NewHTTPSourceClient(WithHTTPClient(expectedHTTPClient))
	suite.Equal(expectedHTTPClient, sourceClient.(*httpSourceClient).httpClient)
	suite.EqualValues(*expectedHTTPClient, *sourceClient.(*httpSourceClient).httpClient)
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientDownloadWithExpire() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	reader, expireInfo, err := suite.DownloadWithExpire(ctx, timeoutURL, map[string]string{})
	cancel()
	suite.NotNil(err)
	suite.Equal("Get \"http://timeout.com\": context deadline exceeded", err.Error())
	suite.Nil(reader)
	suite.Nil(expireInfo)

	type args struct {
		ctx    context.Context
		url    string
		header source.Header
	}
	tests := []struct {
		name       string
		args       args
		content    string
		expireInfo map[string]string
		wantErr    error
	}{
		{
			name: "normal download",
			args: args{
				ctx:    context.Background(),
				url:    normalURL,
				header: nil,
			},
			content: testContent,
			expireInfo: map[string]string{
				headers.LastModified: lastModified,
				headers.ETag:         etag,
			},
			wantErr: nil,
		}, {
			name: "range download",
			args: args{
				ctx:    context.Background(),
				url:    normalURL,
				header: source.Header{"Range": fmt.Sprintf("bytes=%s", "0-3")},
			},
			content: testContent[0:3],
			expireInfo: map[string]string{
				headers.LastModified: lastModified,
				headers.ETag:         etag,
			},
			wantErr: nil,
		}, {
			name: "not found download",
			args: args{
				ctx:    context.Background(),
				url:    notfoundURL,
				header: nil,
			},
			content:    "",
			expireInfo: nil,
			wantErr:    fmt.Errorf("unexpected status code: %d", http.StatusNotFound),
		}, {
			name: "error download",
			args: args{
				ctx:    context.Background(),
				url:    errorURL,
				header: nil,
			},
			content:    "",
			expireInfo: nil,
			wantErr: &url.Error{
				Op:  "Get",
				URL: errorURL,
				Err: fmt.Errorf("error"),
			},
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			reader, expire, err := suite.DownloadWithExpire(tt.args.ctx, tt.args.url, tt.args.header)
			suite.Equal(tt.wantErr, err)
			if err != nil {
				return
			}
			bytes, err := ioutil.ReadAll(reader)
			suite.Nil(err)
			suite.Equal(tt.content, string(bytes))
			suite.Equal(tt.expireInfo, expire)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientGetContentLength() {
	type args struct {
		ctx    context.Context
		url    string
		header source.Header
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr error
	}{
		{name: "support content length", args: args{ctx: context.Background(), url: normalURL, header: map[string]string{}}, want: int64(len(testContent)),
			wantErr: nil},
		{name: "not support content length", args: args{ctx: context.Background(), url: normalURL, header: source.Header{"Range": fmt.Sprintf("bytes=%s",
			"0-3")}}, want: 4,
			wantErr: nil},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.GetContentLength(tt.args.ctx, tt.args.url, tt.args.header)
			suite.Equal(tt.wantErr, err)
			suite.Equal(tt.want, got)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientIsExpired() {
	type args struct {
		ctx        context.Context
		url        string
		header     source.Header
		expireInfo map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{name: "not expire", args: args{context.Background(), normalURL, source.Header{}, map[string]string{headers.LastModified: lastModified,
			headers.ETag: etag}}, want: false, wantErr: false},
		{name: "error not expire", args: args{context.Background(), errorURL, source.Header{}, map[string]string{headers.LastModified: lastModified,
			headers.ETag: etag}}, want: false, wantErr: true},
		{name: "expired", args: args{context.Background(), normalURL, source.Header{}, map[string]string{}}, want: true, wantErr: false},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.IsExpired(tt.args.ctx, tt.args.url, tt.args.header, tt.args.expireInfo)
			suite.Equal(tt.want, got)
			suite.Equal(tt.wantErr, err != nil)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientIsSupportRange() {
	httpmock.RegisterResponder(http.MethodGet, timeoutURL, func(request *http.Request) (*http.Response, error) {
		time.Sleep(3 * time.Second)
		return httpmock.NewStringResponse(http.StatusOK, "ok"), nil
	})
	parent := context.Background()
	ctx, cancel := context.WithTimeout(parent, 1*time.Second)
	support, err := suite.IsSupportRange(ctx, timeoutURL, nil)
	cancel()
	suite.NotNil(err)
	suite.Equal("Get \"http://timeout.com\": context deadline exceeded", err.Error())
	suite.Equal(false, support)
	httpmock.RegisterResponder(http.MethodGet, normalURL, httpmock.NewStringResponder(http.StatusPartialContent, ""))
	httpmock.RegisterResponder(http.MethodGet, "http://notSupportRange.com", httpmock.NewStringResponder(http.StatusOK, ""))
	httpmock.RegisterResponder(http.MethodGet, "http://error.com", httpmock.NewErrorResponder(fmt.Errorf("xxx")))
	type args struct {
		ctx    context.Context
		url    string
		header map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{name: "support", args: args{ctx: context.Background(), url: normalURL, header: source.Header{"Range": fmt.Sprintf("bytes=%s",
			"0-3")}}, want: true, wantErr: false},
		{name: "notSupport", args: args{ctx: context.Background(), url: normalNotSupportRangeURL, header: source.Header{"Range": fmt.Sprintf("bytes=%s",
			"0-3")}}, want: false, wantErr: false},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.IsSupportRange(tt.args.ctx, tt.args.url, tt.args.header)
			suite.Equal(tt.wantErr, err != nil)
			suite.Equal(tt.want, got)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientDoRequest() {
	var testURL = "http://www.hackhttp.com"
	httpmock.RegisterResponder(http.MethodGet, testURL, httpmock.NewStringResponder(http.StatusOK, "ok"))
	res, err := suite.ResourceClient.(*httpSourceClient).doRequest(context.Background(), http.MethodGet, "http://www.hackhttp.com", nil)
	suite.Nil(err)
	bytes, err := ioutil.ReadAll(res.Body)
	suite.Nil(err)
	suite.EqualValues("ok", string(bytes))
}
