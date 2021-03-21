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
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"d7y.io/dragonfly/v2/pkg/structure/maputils"
	"github.com/go-http-utils/headers"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHttpSourceClientTestSuite(t *testing.T) {
	suite.Run(t, new(HttpSourceClientTestSuite))
}

type HttpSourceClientTestSuite struct {
	suite.Suite
	source.ResourceClient
}

func (s *HttpSourceClientTestSuite) SetupSuite() {
	s.ResourceClient = NewHttpSourceClient()
}

func (s *HttpSourceClientTestSuite) TestCopyHeader() {
	type args struct {
		dst map[string]string
		src map[string]string
	}
	tests := []struct {
		name string
		args args
		want map[string]string
	}{
		{
			name: "t1",
			args: args{
				dst: nil,
				src: map[string]string{"Red": "#da1337", "Orange": "#e95a22"},
			},
			want: map[string]string{"Red": "#da1337", "Orange": "#e95a22"},
		}, {
			name: "t2",
			args: args{
				dst: make(map[string]string),
				src: map[string]string{"k1": "v1", "k2": "v2"},
			},
			want: map[string]string{"k1": "v1", "k2": "v2"},
		},
	}
	for _, tt := range tests {
		s.EqualValues(maputils.DeepCopyMap(tt.args.dst, tt.args.src), tt.want)
	}
}

func (s *HttpSourceClientTestSuite) TestGetContentLength() {
	type args struct {
		url    string
		header map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "t1",
			args: args{
				url:    "https://download.jetbrains.8686c.com/go/goland-2020.2.3.dmg",
				header: map[string]string{},
			},
			want:    417880807,
			wantErr: false,
		}, {
			name: "t2",
			args: args{
				url:    "http://www.baidu.com",
				header: map[string]string{},
			},
			want:    277,
			wantErr: false,
		}, {
			name: "t3",
			args: args{
				url: "https://help.aliyun.com/document_detail/31984.html?spm=a2c4g.11186623.6.1696.7899c250peWBw5",
			},
			want:    -1,
			wantErr: false,
		}, {
			name: "t4",
			args: args{
				url: "http://storage-zhangbei.docker.aliyun-inc." +
					"com/docker/registry/v2/blobs/sha256/58" +
					"/5801b9bc7d42e7a6df630c5c35a5eed23ae0ecc963eb4314cc6af3fc4e26ab06/data?Expires=1616228977&OSSAccessKeyId=LTAI4GGexraKrucXWXZfDZxd&Signature=zGi2iB3ghBws5edIJVN5wrRmVnw%3D",
			},
			want:    -1,
			wantErr: false,
		}, {
			name: "t4",
			args: args{
				url:    "http://ossproxy.aone.alibaba-inc.com/aone2/build-service/api/v2/ossproxy/download?ns=Staragent&bucketName=staragent-ui&fileId=plugins/linux/2/DeviceWipe/DeviceWipe.zip.md5&fileName=DeviceWipe.zip.md5&md5Sign=06c14c4b95b7d597d87e846dac4f4b43",
				header: nil,
			},
			want:    -1,
			wantErr: false,
		}, {
			name: "404",
			args: args{
				url: "http://ossproxy.aone.alibaba-inc.com/aone2/build-service/api/v2/ossproxy/download?ns=Staragent&bucketName=staragent-ui&fileId" +
					"=plugins/linux/2/DeviceWipe/DeviceWipe.zip.md5&fileName=D4b43",
				header: nil,
			},
			want:    -1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		got, err := s.GetContentLength(tt.args.url, tt.args.header)
		s.Equal(err != nil, tt.wantErr)
		s.Equal(got, tt.want)
	}
}

func (s *HttpSourceClientTestSuite) TestIsExpired() {
	type args struct {
		url        string
		header     map[string]string
		expireInfo map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "t1",
			args: args{
				url:        "https://download.jetbrains.8686c.com/go/goland-2020.2.3.dmg",
				expireInfo: map[string]string{"Etag": "lmW9EEXRsIpgQHKGyHMYFxFZBaJ1", "Last-Modified": "Wed, 16 Sep 2020 11:58:38 GMT"},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		got, err := s.IsExpired(tt.args.url, tt.args.header, tt.args.expireInfo)
		s.Equal(err != nil, tt.wantErr)
		s.Equal(got, tt.want)
	}
}

func (s *HttpSourceClientTestSuite) TestIsSupportRange() {
	type args struct {
		url    string
		header map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "support",
			args: args{
				url:    "https://download.jetbrains.8686c.com/go/goland-2020.2.3.dmg",
				header: nil,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "notSupport",
			args: args{
				url:    "https://image.baidu.com/search/down?tn=download&ipn=dwnl&word=download&ie=utf8&fr=result&url=http%3A%2F%2Fsrc.onlinedown.net%2Fsupply%2F1372064088_17046.png&thumburl=https%3A%2F%2Fss0.bdstatic.com%2F70cFvHSh_Q1YnxGkpoWK1HF6hhy%2Fit%2Fu%3D3211174755%2C200170773%26fm%3D26%26gp%3D0.jpg",
				header: nil,
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		got, err := s.IsSupportRange(tt.args.url, tt.args.header)
		s.Equal(err != nil, tt.wantErr)
		s.Equal(got, tt.want)
	}
}

func (s *HttpSourceClientTestSuite) TestDownload() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Equal(r.Method, "GET")
		w.WriteHeader(http.StatusOK)
		w.Header().Set(headers.LastModified, "Wed, 16 Sep 2020 11:58:38 GMT")
		w.Header().Set(headers.ETag, "lmW9EEXRsIpgQHKGyHMYFxFZBaJ1")
		s.Equal(r.URL.EscapedPath(), "/test")
		w.Write([]byte("test"))

	}))

	defer ts.Close()

	type args struct {
		url    string
		header map[string]string
	}
	tests := []struct {
		name       string
		args       args
		content    string
		expireInfo map[string]string
		wantErr    error
	}{
		{
			name: "t1",
			args: args{
				url:    ts.URL + "/test",
				header: nil,
			},
			content: "test",
			expireInfo: map[string]string{
				headers.LastModified: "Wed, 16 Sep 2020 11:58:38 GMT",
				headers.ETag:         "lmW9EEXRsIpgQHKGyHMYFxFZBaJ1",
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		reader, expire, err := s.Download(tt.args.url, tt.args.header)
		s.Equal(tt.wantErr, err)
		bytes, err := ioutil.ReadAll(reader)
		s.Nil(err)
		s.Equal(tt.content, string(bytes))
		s.Equal(tt.expireInfo, expire)
	}
}

//func (s *HttpSourceClientTestSuite) Test_httpSourceClient_Download() {

//
//	type args struct {
//		url     string
//		header map[string]string
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *types.DownloadResponse
//		wantErr bool
//	}{
//		{
//			name: "t1",
//			args: args{
//				url: ts.URL,
//			},
//			want: &types.DownloadResponse{
//				Body:       nil,
//				ExpireInfo: nil,
//			},
//			wantErr: false,
//		},
//		{
//			name: "t2",
//			args: args{
//				url: "https://download.jetbrains.8686c.com/go/goland-2020.2.3.dmg",
//			},
//			want: &types.DownloadResponse{
//				Body:       nil,
//				ExpireInfo: nil,
//			},
//			wantErr: false,
//		},
//	}
//	for _, tt := range tests {
//		got, err := s.client.Download(tt.args.url, tt.args.header)
//		s.Equal(err != nil, tt.wantErr)
//		s.Equal(got, tt.want)
//	}
//}
