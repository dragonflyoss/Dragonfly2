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
	"d7y.io/dragonfly/v2/cdnsystem/source"
	"fmt"
	"github.com/go-http-utils/headers"
	"github.com/stretchr/testify/suite"
	"io/ioutil"
	"testing"
)

func TestOssSourceClientTestSuite(t *testing.T) {
	suite.Run(t, new(OssSourceClientTestSuite))
}

type OssSourceClientTestSuite struct {
	suite.Suite
	source.ResourceClient
}

func (s *OssSourceClientTestSuite) SetupSuite() {
	s.ResourceClient = NewOSSSourceClient()
}

func (s *OssSourceClientTestSuite) TeardownSuite() {
	fmt.Println("teardownSuite")
}

func (s *OssSourceClientTestSuite) SetupTest() {
	fmt.Println("setupTest")
}

func (s *OssSourceClientTestSuite) TeardownTest() {
	fmt.Println("teardownTest")
}

func (s *OssSourceClientTestSuite) TestParseOssObject() {
	type args struct {
		ossUrl string
	}
	tests := []struct {
		name    string
		args    args
		want    *ossObject
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				ossUrl: "oss://alimonitor-monitor/rowkey_20191010144421mYtlKyATuW_app.txt",
			},
			want: &ossObject{
				bucket: "alimonitor-monitor",
				object: "rowkey_20191010144421mYtlKyATuW_app.txt",
			},
			wantErr: false,
		}, {
			name: "test2",
			args: args{
				ossUrl: "oss://alimonitor-monitor/aaa/rowkey_20191010144421mYtlKyATuW_app.txt",
			},
			want: &ossObject{
				bucket: "alimonitor-monitor",
				object: "aaa/rowkey_20191010144421mYtlKyATuW_app.txt",
			},
			wantErr: false,
		}, {
			name: "test3",
			args: args{
				ossUrl: "http://alimonitor-monitor/aaa/rowkey_20191010144421mYtlKyATuW_app.txt",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			got, err := ParseOssObject(tt.args.ossUrl)
			s.Equal(tt.want, got)
			s.Equal(tt.wantErr, err != nil)
		})
	}
}

func (s *OssSourceClientTestSuite) TestDownload() {
	type args struct {
		url    string
		header map[string]string
	}
	tests := []struct {
		name       string
		args       args
		content    string
		expireInfo map[string]string
		wantErr    bool
	}{
		{
			name: "full",
			args: args{
				url: "oss://alimonitor-monitor/sss/ddd",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
			},
			content: "aaaaa\n",
			expireInfo: map[string]string{
				headers.LastModified: "Sun, 28 Mar 2021 11:23:45 GMT",
				headers.ETag:         "\"4C850C5B3B2756E67A91BAD8E046DDAC\"",
			},
			wantErr: false,
		}, {
			name: "range",
			args: args{
				url: "oss://alimonitor-monitor/sss/xxx",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
					"range":           "bytes=0-3",
				},
			},
			content: "aaaa",
			expireInfo: map[string]string{
				headers.LastModified: "Sun, 28 Mar 2021 11:43:08 GMT",
				headers.ETag:         "\"D466C9A11362E24DA6420A3AD0421A89\"",
			},
			wantErr: false,
		}, {
			name: "error",
			args: args{
				url: "oss://alimonitor-monitor/sss/xxdx",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
					"range":           "bytes=0-3",
				},
			},
			content: "aaaa",
			expireInfo: map[string]string{
				headers.LastModified: "Sun, 28 Mar 2021 11:43:08 GMT",
				headers.ETag:         "\"D466C9A11362E24DA6420A3AD0421A89\"",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			got, expireInfo, err := s.Download(tt.args.url, tt.args.header)
			s.Equal(tt.wantErr, err != nil)
			if err != nil {
				return
			}
			defer got.Close()
			data, err := ioutil.ReadAll(got)
			s.Equal(tt.expireInfo, expireInfo)
			s.Equal(tt.content, string(data))
		})
	}
}

func (s *OssSourceClientTestSuite) TestGetContentLength() {
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
			name: "test1",
			args: args{
				url: "oss://alimonitor-monitor/rowkey_20191010144421mYtlKyATuW_app.txt",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
			},
			want: 287329086,
		}, {
			name: "test1",
			args: args{
				url: "oss://alimonitor-monitor/sss/ddd",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
			},
			want: 6,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			got, err := s.GetContentLength(tt.args.url, tt.args.header)
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.want, got)
		})
	}
}

func (s *OssSourceClientTestSuite) TestsExpired() {
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
			name: "test1",
			args: args{
				url: "oss://alimonitor-monitor/sss/ddd",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
				expireInfo: map[string]string{
					headers.LastModified: "Sun, 28 Mar 2021 11:23:45 GMT",
					headers.ETag:         "\"4C850C5B3B2756E67A91BAD8E046DDAC\"",
				},
			},
			want: true,
		}, {
			name: "test1",
			args: args{
				url: "oss://alimonitor-monitor/sss/ddd",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
				expireInfo: map[string]string{
					headers.LastModified: "Sun, 283 Mar 2021 11:23:45 GMT",
					headers.ETag:         "\"4C850C5B3B2756E67A91BAD8E046DDAC\"",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			got, err := s.IsExpired(tt.args.url, tt.args.header, tt.args.expireInfo)
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.want, got)
		})
	}
}

func (s *OssSourceClientTestSuite) TestIsSupportRange() {
	type args struct {
		url     string
		headers map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "test1",
			args: args{
				url: "oss://alimonitor-monitor/rowkey_20191010144421mYtlKyATuW_app.txt",
				headers: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			got, err := s.IsSupportRange(tt.args.url, tt.args.headers)
			s.Equal(tt.want, got)
			s.Equal(tt.wantErr, err != nil)
		})
	}
}
