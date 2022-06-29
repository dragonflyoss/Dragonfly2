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

package dfget

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/source"
	"d7y.io/dragonfly/v2/pkg/source/mocks"
)

func Test_downloadFromSource(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	assert.Nil(t, err)
	output := filepath.Join(homeDir, idgen.UUIDString())
	defer os.Remove(output)

	content := idgen.UUIDString()

	sourceClient := mocks.NewMockResourceClient(gomock.NewController(t))
	require.Nil(t, source.Register("http", sourceClient, func(request *source.Request) *source.Request {
		return request
	}))
	defer source.UnRegister("http")

	cfg := &config.DfgetConfig{
		URL:    "http://a.b.c/xx",
		Output: output,
		Digest: strings.Join([]string{digest.AlgorithmSHA256, digest.SHA256FromStrings(content)}, ":"),
	}
	request, err := source.NewRequest(cfg.URL)
	assert.Nil(t, err)
	sourceClient.EXPECT().Download(request).Return(source.NewResponse(io.NopCloser(strings.NewReader(content))), nil)

	err = downloadFromSource(context.Background(), cfg, nil)
	assert.Nil(t, err)
}

func Test_checkDirectoryLevel(t *testing.T) {
	testCases := []struct {
		parent string
		sub    string
		level  uint
		ok     bool
	}{
		{
			parent: "/data/",
			sub:    "/data/1/2/3/4/5",
			level:  0,
			ok:     true,
		},
		{
			parent: "/data/",
			sub:    "/data/1",
			level:  1,
			ok:     true,
		},
		{
			parent: "/data/",
			sub:    "/data/1/2",
			level:  1,
			ok:     false,
		},
		{
			parent: "/data/",
			sub:    "/data/1/2/3/4/5",
			level:  5,
			ok:     true,
		},
		{
			parent: "/data/",
			sub:    "/data/1/2/3/4/5",
			level:  4,
			ok:     false,
		},
		{
			parent: "/d7y-test",
			sub:    "/d7y-test/dir/file",
			level:  1,
			ok:     false,
		},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			ok := checkDirectoryLevel(tc.parent, tc.sub, tc.level)
			assert.Equal(t, tc.ok, ok)
		})
	}
}
