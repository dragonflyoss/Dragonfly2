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

package storedriver

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/cdn/plugins"
	"d7y.io/dragonfly/v2/pkg/unit"
)

type mockDriver struct {
	BaseDir string
}

func newDriver(cfg *Config) (Driver, error) {
	return &mockDriver{
		BaseDir: cfg.BaseDir,
	}, nil
}

func (m mockDriver) Get(_ *Raw) (io.ReadCloser, error) {
	panic("implement me")
}

func (m mockDriver) GetBytes(_ *Raw) ([]byte, error) {
	panic("implement me")
}

func (m mockDriver) Put(_ *Raw, _ io.Reader) error {
	panic("implement me")
}

func (m mockDriver) PutBytes(_ *Raw, _ []byte) error {
	panic("implement me")
}

func (m mockDriver) Remove(_ *Raw) error {
	panic("implement me")
}

func (m mockDriver) Stat(_ *Raw) (*StorageInfo, error) {
	panic("implement me")
}

func (m mockDriver) GetFreeSpace() (unit.Bytes, error) {
	panic("implement me")
}

func (m mockDriver) GetTotalAndFreeSpace() (unit.Bytes, unit.Bytes, error) {
	panic("implement me")
}

func (m mockDriver) GetTotalSpace() (unit.Bytes, error) {
	panic("implement me")
}

func (m mockDriver) Walk(_ *Raw) error {
	panic("implement me")
}

func (m mockDriver) CreateBaseDir() error {
	panic("implement me")
}

func (m mockDriver) GetPath(_ *Raw) string {
	panic("implement me")
}

func (m mockDriver) MoveFile(_ string, _ string) error {
	panic("implement me")
}

func (m mockDriver) Exits(_ *Raw) bool {
	panic("implement me")
}

func (m mockDriver) GetBaseDir() string {
	panic("implement me")
}

func TestRegister(t *testing.T) {
	assert := assert.New(t)
	var pluginName = "mock"
	var baseDir = "/tmp/drivertest"
	var driverBuilder = newDriver
	err := Register(pluginName, driverBuilder)
	assert.Nil(err)
	driver, ok := Get(pluginName)
	assert.Nil(driver)
	assert.Equal(false, ok)
	// enable driver
	err = plugins.Initialize(map[plugins.PluginType][]*plugins.PluginProperties{
		plugins.StorageDriverPlugin: {
			&plugins.PluginProperties{
				Name:   pluginName,
				Enable: true,
				Config: &Config{
					baseDir,
				},
			},
		},
	})
	assert.Nil(err)
	driver, ok = Get("mock")
	assert.Equal(&mockDriver{
		BaseDir: baseDir,
	}, driver)
	assert.Equal(true, ok)
	assert.Nil(os.Remove(baseDir))
}
