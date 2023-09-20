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
	"io"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/go-http-utils/headers"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/suite"

	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/source"
)

func TestHTTPSourceClientTestSuite(t *testing.T) {
	suite.Run(t, new(HTTPSourceClientTestSuite))
}

type HTTPSourceClientTestSuite struct {
	suite.Suite
	httpClient *httpSourceClient
}

func (suite *HTTPSourceClientTestSuite) SetupSuite() {
	suite.httpClient = newHTTPSourceClient()
	httpmock.ActivateNonDefault(suite.httpClient.httpClient)
}

func (suite *HTTPSourceClientTestSuite) TearDownSuite() {
	httpmock.DeactivateAndReset()
}

var (
	timeoutRawURL               = "https://timeout.com"
	normalRawURL                = "https://normal.com"
	expireRawURL                = "https://expired.com"
	errorRawURL                 = "https://error.com"
	forbiddenRawURL             = "https://forbidden.com"
	notfoundRawURL              = "https://notfound.com"
	normalNotSupportRangeRawURL = "https://notsuppertrange.com"
)

var (
	testContent        = "l am test case"
	lastModified       = "Sun, 06 Jun 2021 12:52:30 GMT"
	expireLastModified = "Sun, 06 Jun 2021 11:52:30 GMT"
	etag               = "UMiJT4h7MCEAEgnqCLA2CdAaABnK"
	expireEtag         = "UMiJ2T4h7MCEAEgnqCLA2CdAaABnK"
)

func (suite *HTTPSourceClientTestSuite) SetupTest() {
	httpmock.Reset()
	httpmock.RegisterResponder(http.MethodGet, timeoutRawURL, func(request *http.Request) (*http.Response, error) {
		// To simulate the timeout
		time.Sleep(5 * time.Second)
		return httpmock.NewStringResponse(http.StatusOK, "ok"), nil
	})

	httpmock.RegisterResponder(http.MethodGet, normalRawURL, func(request *http.Request) (*http.Response, error) {
		if rg := request.Header.Get(headers.Range); rg != "" {
			r, _ := nethttp.ParseOneRange(rg, math.MaxInt64)
			header := http.Header{}
			header.Set(headers.LastModified, lastModified)
			header.Set(headers.ETag, etag)
			res := &http.Response{
				StatusCode:    http.StatusPartialContent,
				ContentLength: r.Length,
				Body:          httpmock.NewRespBodyFromString(testContent[r.Start : r.Start+r.Length-1]),
				Header:        header,
			}
			return res, nil
		}
		if expire := request.Header.Get(headers.IfModifiedSince); expire != "" {
			header := http.Header{}
			header.Set(headers.LastModified, lastModified)
			header.Set(headers.ETag, etag)
			res := &http.Response{
				StatusCode:    http.StatusNotModified,
				ContentLength: int64(len(testContent)),
				Body:          httpmock.NewRespBodyFromString(testContent),
				Header:        header,
			}
			return res, nil
		}
		header := http.Header{}
		header.Set(headers.LastModified, lastModified)
		header.Set(headers.ETag, etag)
		res := &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: 14,
			Body:          httpmock.NewRespBodyFromString(testContent),
			Header:        header,
		}
		return res, nil
	})

	header := http.Header{}
	header.Set(headers.LastModified, lastModified)
	header.Set(headers.ETag, etag)
	httpmock.RegisterResponder(http.MethodGet, expireRawURL, func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: 14,
			Body:          httpmock.NewRespBodyFromString(testContent),
			Header:        header,
		}, nil
	})

	httpmock.RegisterResponder(http.MethodGet, forbiddenRawURL, httpmock.NewStringResponder(http.StatusForbidden, "forbidden"))
	httpmock.RegisterResponder(http.MethodGet, notfoundRawURL, httpmock.NewStringResponder(http.StatusNotFound, "not found"))
	httpmock.RegisterResponder(http.MethodGet, normalNotSupportRangeRawURL, httpmock.NewStringResponder(http.StatusOK, testContent))
	httpmock.RegisterResponder(http.MethodGet, errorRawURL, httpmock.NewErrorResponder(fmt.Errorf("error")))
}

func (suite *HTTPSourceClientTestSuite) TestNewHTTPSourceClient() {
	var sourceClient source.ResourceClient
	sourceClient = NewHTTPSourceClient()
	suite.Equal(http.DefaultClient, sourceClient.(*httpSourceClient).httpClient)
	suite.EqualValues(*http.DefaultClient, *sourceClient.(*httpSourceClient).httpClient)

	expectedHTTPClient := &http.Client{}
	sourceClient = NewHTTPSourceClient(WithHTTPClient(expectedHTTPClient))
	suite.Equal(expectedHTTPClient, sourceClient.(*httpSourceClient).httpClient)
	suite.EqualValues(*expectedHTTPClient, *sourceClient.(*httpSourceClient).httpClient)
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientDownloadWithResponseHeader() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	timeoutRequest, err := source.NewRequestWithContext(ctx, timeoutRawURL, nil)
	suite.Nil(err)
	response, err := suite.httpClient.Download(timeoutRequest)
	cancel()
	suite.NotNil(err)
	suite.Equal("Get \"https://timeout.com\": context deadline exceeded", err.Error())
	suite.Nil(response)

	normalRequest, _ := source.NewRequest(normalRawURL)
	normalRangeRequest, _ := source.NewRequest(normalRawURL)
	normalRangeRequest.Header.Add(headers.Range, fmt.Sprintf("bytes=%s", "0-3"))
	notfoundRequest, _ := source.NewRequest(notfoundRawURL)
	errorRequest, _ := source.NewRequest(errorRawURL)
	tests := []struct {
		name       string
		request    *source.Request
		content    string
		expireInfo *source.ExpireInfo
		wantErr    error
	}{
		{
			name:    "normal download",
			request: normalRequest,
			content: testContent,
			expireInfo: &source.ExpireInfo{
				LastModified: lastModified,
				ETag:         etag,
			},
			wantErr: nil,
		}, {
			name:    "range download",
			request: normalRangeRequest,
			content: testContent[0:3],
			expireInfo: &source.ExpireInfo{
				LastModified: lastModified,
				ETag:         etag,
			},
			wantErr: nil,
		}, {
			name:       "not found download",
			request:    notfoundRequest,
			content:    "",
			expireInfo: nil,
			wantErr:    source.CheckResponseCode(404, []int{200, 206}),
		}, {
			name:       "error download",
			request:    errorRequest,
			content:    "",
			expireInfo: nil,
			wantErr:    fmt.Errorf("Get \"https://error.com\": error"),
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			response, err := suite.httpClient.Download(tt.request)
			if err != nil {
				suite.True(tt.wantErr.Error() == err.Error())
				return
			}
			if err = response.Validate(); err != nil {
				suite.True(tt.wantErr.Error() == err.Error())
				return
			}
			bytes, err := io.ReadAll(response.Body)
			suite.Nil(err)
			suite.Equal(tt.content, string(bytes))
			expireInfo := response.ExpireInfo()
			suite.Equal(tt.expireInfo, &expireInfo)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientGetContentLength() {
	normalRequest, _ := source.NewRequest(normalRawURL)
	normalRangeRequest, _ := source.NewRequest(normalRawURL)
	normalRangeRequest.Header.Add(headers.Range, fmt.Sprintf("bytes=%s", "0-3"))
	tests := []struct {
		name    string
		request *source.Request
		want    int64
		wantErr error
	}{
		{name: "support content length", request: normalRequest,
			want:    int64(len(testContent)),
			wantErr: nil},
		{name: "not support content length", request: normalRangeRequest,
			want:    4,
			wantErr: nil},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.httpClient.GetContentLength(tt.request)
			suite.Equal(tt.wantErr, err)
			suite.Equal(tt.want, got)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientIsExpired() {
	normalRequest, _ := source.NewRequest(normalRawURL)
	errorRequest, _ := source.NewRequest(errorRawURL)
	expireRequest, _ := source.NewRequest(expireRawURL)
	tests := []struct {
		name       string
		request    *source.Request
		expireInfo *source.ExpireInfo
		want       bool
		wantErr    bool
	}{
		{name: "not expire", request: normalRequest, expireInfo: &source.ExpireInfo{
			LastModified: lastModified,
			ETag:         etag,
		}, want: false, wantErr: false},
		{name: "error not expire", request: errorRequest, expireInfo: &source.ExpireInfo{
			LastModified: lastModified,
			ETag:         etag,
		}, want: false, wantErr: true},
		{name: "expired", request: expireRequest, expireInfo: &source.ExpireInfo{
			LastModified: expireLastModified,
			ETag:         expireEtag,
		}, want: true, wantErr: false},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.httpClient.IsExpired(tt.request, tt.expireInfo)
			suite.Equal(tt.want, got)
			suite.Equal(tt.wantErr, err != nil)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientIsSupportRange() {
	httpmock.RegisterResponder(http.MethodGet, timeoutRawURL, func(request *http.Request) (*http.Response, error) {
		time.Sleep(3 * time.Second)
		return httpmock.NewStringResponse(http.StatusOK, "ok"), nil
	})
	parent := context.Background()
	ctx, cancel := context.WithTimeout(parent, 1*time.Second)
	request, err := source.NewRequestWithContext(ctx, timeoutRawURL, nil)
	suite.Nil(err)
	support, err := suite.httpClient.IsSupportRange(request)
	cancel()
	suite.NotNil(err)
	suite.Equal("Get \"https://timeout.com\": context deadline exceeded", err.Error())
	suite.Equal(false, support)
	httpmock.RegisterResponder(http.MethodGet, normalRawURL, httpmock.NewStringResponder(http.StatusPartialContent, ""))
	httpmock.RegisterResponder(http.MethodGet, normalNotSupportRangeRawURL, httpmock.NewStringResponder(http.StatusOK, ""))
	httpmock.RegisterResponder(http.MethodGet, errorRawURL, httpmock.NewErrorResponder(fmt.Errorf("xxx")))

	supportRangeRequest, _ := source.NewRequest(normalRawURL)
	supportRangeRequest.Header.Add(headers.Range, fmt.Sprintf("bytes=%s", "0-3"))
	notSupportRangeURL, _ := source.NewRequest(normalNotSupportRangeRawURL)
	notSupportRangeURL.Header.Add(headers.Range, fmt.Sprintf("bytes=%s", "0-3"))
	errRequest, _ := source.NewRequest(errorRawURL)
	tests := []struct {
		name    string
		request *source.Request
		want    bool
		wantErr bool
	}{
		{name: "support", request: supportRangeRequest, want: true, wantErr: false},
		{name: "notSupport", request: notSupportRangeURL, want: false, wantErr: false},
		{name: "error", request: errRequest, want: false, wantErr: true},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := suite.httpClient.IsSupportRange(tt.request)
			suite.Equal(tt.wantErr, err != nil)
			suite.Equal(tt.want, got)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientGetLastModified() {
	tests := []struct {
		name   string
		mock   func(testURL string)
		expect func(timeStamp int64)
	}{
		{
			name: "request with last modified",
			mock: func(testURL string) {
				httpmock.RegisterResponder(http.MethodGet, testURL, func(request *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header: http.Header{
							"Last-Modified": []string{"Mon, 23 Jan 2017 13:28:11 GMT"},
						},
					}, nil
				})
			},
			expect: func(timeStamp int64) {
				suite.EqualValues(1485178091000, timeStamp)
			},
		},
		{
			name: "error request",
			mock: func(testURL string) {
				httpmock.RegisterResponder(http.MethodGet, testURL, httpmock.NewErrorResponder(fmt.Errorf("foo")))
			},
			expect: func(timeStamp int64) {
				suite.EqualValues(-1, timeStamp)
			},
		},
		{
			name: "request not found",
			mock: func(testURL string) {
				httpmock.RegisterResponder(http.MethodGet, testURL, httpmock.NewStringResponder(http.StatusNotFound, "not found"))
			},
			expect: func(timeStamp int64) {
				suite.EqualValues(-1, timeStamp)
			},
		},
		{
			name: "request without last modified",
			mock: func(testURL string) {
				httpmock.RegisterResponder(http.MethodGet, testURL, httpmock.NewStringResponder(http.StatusOK, "ok"))
			},
			expect: func(timeStamp int64) {
				suite.EqualValues(-1, timeStamp)
			},
		},
		{
			name: "request with error last modified",
			mock: func(testURL string) {
				httpmock.RegisterResponder(http.MethodGet, testURL, func(request *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header: http.Header{
							"Last-Modified": []string{"Thu Feb  4 21:00:57.01 PST 2010"},
						},
					}, nil
				})
			},
			expect: func(timeStamp int64) {
				suite.EqualValues(-1, timeStamp)
			},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			var testURL = "https://www.example.com"
			tt.mock(testURL)

			request, _ := source.NewRequest(testURL)
			timeStamp, _ := suite.httpClient.GetLastModified(request)
			tt.expect(timeStamp)
		})
	}
}

func (suite *HTTPSourceClientTestSuite) TestHttpSourceClientDoRequest() {
	var testURL = "https://www.hackhttp.com"
	httpmock.RegisterResponder(http.MethodGet, testURL, httpmock.NewStringResponder(http.StatusOK, "ok"))
	request, err := source.NewRequest(testURL)
	suite.Nil(err)
	res, err := suite.httpClient.doRequest(http.MethodGet, request)
	suite.Nil(err)
	bytes, err := io.ReadAll(res.Body)
	suite.Nil(err)
	suite.EqualValues("ok", string(bytes))
}
