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
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/source"
	"github.com/dragonflyoss/Dragonfly2/cdnsystem/types"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/maputils"
	"reflect"
	"testing"
)

func TestCopyHeader(t *testing.T) {
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
			"t2",
			args{
				dst: make(map[string]string),
				src: map[string]string{"k1": "v1", "k2": "v2"},
			},
			map[string]string{"k1": "v1", "k2": "v2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := maputils.DeepCopyMap(tt.args.dst, tt.args.src); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CopyHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_httpSourceClient_Download(t *testing.T) {
	type args struct {
		url       string
		headers   map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    *types.DownloadResponse
		wantErr bool
	}{
		{
			name: "t1",
			args: args{
				url:       "https://download.jetbrains.8686c.com/go/goland-2020.2.3.dmg",
			},
			want: &types.DownloadResponse{
				Body:       nil,
				ExpireInfo: nil,
			},
			wantErr: false,
		},
	}
	client := NewHttpSourceClient()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.Download(tt.args.url, tt.args.headers)
			if (err != nil) != tt.wantErr {
				t.Errorf("Download() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			fmt.Printf("%+v", got)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Download() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_httpSourceClient_GetContentLength(t *testing.T) {
	type args struct {
		url     string
		headers map[string]string
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
				url:     "https://download.jetbrains.8686c.com/go/goland-2020.2.3.dmg",
				headers: map[string]string{},
			},
			want:    417880807,
			wantErr: false,
		},
	}
	client := NewHttpSourceClient()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.GetContentLength(tt.args.url, tt.args.headers)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetContentLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetContentLength() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_httpSourceClient_IsExpired(t *testing.T) {
	type args struct {
		url        string
		headers    map[string]string
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
	client := NewHttpSourceClient()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.IsExpired(tt.args.url, tt.args.headers, tt.args.expireInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsExpired() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IsExpired() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_httpSourceClient_IsSupportRange(t *testing.T) {
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
			name: "support",
			args: args{
				url:     "https://download.jetbrains.8686c.com/go/goland-2020.2.3.dmg",
				headers: nil,
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "notSupport",
			args: args{
				url:     "https://image.baidu.com/search/down?tn=download&ipn=dwnl&word=download&ie=utf8&fr=result&url=http%3A%2F%2Fsrc.onlinedown.net%2Fsupply%2F1372064088_17046.png&thumburl=https%3A%2F%2Fss0.bdstatic.com%2F70cFvHSh_Q1YnxGkpoWK1HF6hhy%2Fit%2Fu%3D3211174755%2C200170773%26fm%3D26%26gp%3D0.jpg",
				headers: nil,
			},
			want:    true,
			wantErr: false,
		},
	}
	client := NewHttpSourceClient()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.IsSupportRange(tt.args.url, tt.args.headers)
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

func Test_newHttpSourceClient(t *testing.T) {
	tests := []struct {
		name    string
		want    source.ResourceClient
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewHttpSourceClient()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newHttpSourceClient() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func checkStatusCode(statusCode []int) func(int) bool {
	return func(status int) bool {
		for _, s := range statusCode {
			if status == s {
				return true
			}
		}
		return false
	}
}
