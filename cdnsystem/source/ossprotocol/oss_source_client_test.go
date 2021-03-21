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
	"net/http"
	"reflect"
	"sync"
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
		}, {
			name: "test2",
			args: args{
				ossUrl: "oss://alimonitor-monitor/aaa/rowkey_20191010144421mYtlKyATuW_app.txt",
			},
			want: &ossObject{
				bucket: "alimonitor-monitor",
				object: "aaa/rowkey_20191010144421mYtlKyATuW_app.txt",
			},
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
			if err != nil {
				s.Nil(got)
				s.Equal(err, tt.wantErr)
			} else {
				s.Equal(got, tt.want)
			}
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
		wantErr    error
	}{
		{
			name: "t1",
			args: args{
				url: "oss://alimonitor-monitor/server.xml",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
			},
			content: "test",
			expireInfo: map[string]string{
				headers.LastModified: "Wed, 16 Sep 2020 11:58:38 GMT",
				headers.ETag:         "lmW9EEXRsIpgQHKGyHMYFxFZBaJ1",
			},
			wantErr: nil,
		}, {
			name: "test1",
			args: args{
				url: "oss://alimonitor-monitor/server.xml",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
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
		s.Run(tt.name, func() {
			got, expireInfo, err := s.Download(tt.args.url, tt.args.header)
			s.Equal(tt.wantErr, err != nil)
			if err != nil {
				return
			}
			defer got.Close()
			data, err := ioutil.ReadAll(got)
			s.Nil(err)
			s.Equal(tt.expireInfo, expireInfo)
			fmt.Println("data:", string(data))
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
				url: "oss://alimonitor-monitor/rowkey_20191010144421mYtlKyATuW_app.txt",
				header: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
				expireInfo: map[string]string{},
			},
			want: true,
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

func Test_ossSourceClient_IsSupportRange(t *testing.T) {
	type fields struct {
		clientMap sync.Map
		accessMap sync.Map
	}
	type args struct {
		url     string
		headers map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			osc := &ossSourceClient{
				clientMap: tt.fields.clientMap,
				accessMap: tt.fields.accessMap,
			}
			got, err := osc.IsSupportRange(tt.args.url, tt.args.headers)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsSupportRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IsSupportRange() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ossSourceClient_getMeta(t *testing.T) {
	type fields struct {
		clientMap sync.Map
		accessMap sync.Map
	}
	type args struct {
		url     string
		headers map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    http.Header
		wantErr bool
	}{
		{
			name: "test1",
			fields: fields{
				clientMap: sync.Map{},
				accessMap: sync.Map{},
			},
			args: args{
				url: "oss://alimonitor-monitor/rowkey_20191010144421mYtlKyATuW_app.txt",
				headers: map[string]string{
					"endpoint":        "http://oss-cn-hangzhou-zmf.aliyuncs.com",
					"accessKeyID":     "RX8yefyaWDWf15SV",
					"accessKeySecret": "hPExQDzDPHepZA7W6N5U7skJqLZGhy",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			osc := &ossSourceClient{
				clientMap: tt.fields.clientMap,
				accessMap: tt.fields.accessMap,
			}
			got, err := osc.getMeta(tt.args.url, tt.args.headers)
			if (err != nil) != tt.wantErr {
				t.Errorf("getMeta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getMeta() got = %v, want %v", got, tt.want)
			}
		})
	}
}
