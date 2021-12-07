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

package task

import (
	"net/url"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/jarcoal/httpmock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"d7y.io/dragonfly/v2/cdn/config"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/httpprotocol"
	sourcemock "d7y.io/dragonfly/v2/pkg/source/mock"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestIsTaskNotFound(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "wrap task not found error",
			args: args{
				err: errors.Wrap(errTaskNotFound, "wrap error"),
			},
			want: true,
		}, {
			name: "wrap task two layers",
			args: args{
				err: errors.Wrap(errors.Wrap(errTaskNotFound, "wrap error"), "wrap error again"),
			},
			want: true,
		}, {
			name: "native err",
			args: args{
				err: errTaskNotFound,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTaskNotFound(tt.args.err); got != tt.want {
				t.Errorf("IsTaskNotFound() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_manager_Exist(t *testing.T) {
	httpmock.Activate()
	tm, err := NewManager(config.New())
	require := require.New(t)
	require.Nil(err)
	ctl := gomock.NewController(t)
	sourceClient := sourcemock.NewMockResourceClient(ctl)
	testURL, err := url.Parse("https://dragonfly.com")
	require.Nil(err)
	source.UnRegister("https")
	require.Nil(source.Register("https", sourceClient, httpprotocol.Adapter))

	sourceClient.EXPECT().GetContentLength(source.RequestEq(testURL.String())).Return(int64(1024*1024*500+1000), nil).Times(1)
	seedTask := NewSeedTask("taskID", testURL.String(), nil)
	addedTask, err := tm.AddOrUpdate(seedTask)
	require.Nil(err)
	existTask, ok := tm.Exist("taskID")
	require.True(ok)
	require.EqualValues(addedTask, existTask)
	require.EqualValues(1024*1024*500+1000, existTask.SourceFileLength)
	require.EqualValues(1024*1024*7, existTask.PieceSize)
}
