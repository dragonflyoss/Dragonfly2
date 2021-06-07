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

package dfplugin

import (
	"io"
	"io/ioutil"
	"testing"

	"d7y.io/dragonfly/v2/internal/dfpath"
	testifyassert "github.com/stretchr/testify/assert"
)

const (
	PluginTypeTest = PluginType("test")
)

type testPlugin interface {
	GetContentLength(url string, headers map[string]string) (int64, error)
	Download(url string, headers map[string]string) (io.ReadCloser, map[string]string, error)
}

// when run test in debug, please add "-gcflags=all=-N -gcflags=all=-l" after "go build", and add "-race"
//go:generate go build -race -buildmode=plugin -o=./testdata/d7y-test-plugin-df.so testdata/plugin/df.go
func Test_Load(t *testing.T) {
	dfpath.PluginsDir = "./testdata"
	assert := testifyassert.New(t)
	p, meta, err := Load(PluginTypeTest, "df", map[string]string{})
	assert.Nil(err)
	if err != nil {
		return
	}
	assert.Equal("df", meta["name"])

	client, ok := p.(testPlugin)
	assert.True(ok)

	l, err := client.GetContentLength("", nil)
	assert.Nil(err)

	r, _, err := client.Download("", nil)
	assert.Nil(err)

	data, err := ioutil.ReadAll(r)
	assert.Nil(err)
	assert.Equal(l, int64(len(data)))

	err = r.Close()
	assert.Nil(err)
}
