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

package config

import (
	"io/ioutil"
	"testing"

	"github.com/mitchellh/mapstructure"
	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestManagerConfig_Load(t *testing.T) {
	assert := testifyassert.New(t)

	config := &Config{
		Server: &ServerConfig{
			Name: "foo",
			GRPC: &TCPListenConfig{
				Listen: "127.0.0.1",
				PortRange: TCPListenPortRange{
					Start: 65003,
					End:   65003,
				},
			},
			REST: &RestConfig{
				Addr: ":8080",
			},
		},
		Database: &DatabaseConfig{
			Mysql: &MysqlConfig{
				User:     "foo",
				Password: "foo",
				Host:     "foo",
				Port:     3306,
				DBName:   "foo",
			},
			Redis: &RedisConfig{
				Host:      "bar",
				Password:  "bar",
				Port:      6379,
				CacheDB:   0,
				BrokerDB:  1,
				BackendDB: 2,
			},
		},
		Cache: &CacheConfig{
			Redis: &RedisCacheConfig{
				TTL: 1000,
			},
			Local: &LocalCacheConfig{
				Size: 10000,
				TTL:  1000,
			},
		},
	}

	managerConfigYAML := &Config{}
	contentYAML, _ := ioutil.ReadFile("./testdata/manager.yaml")
	var dataYAML map[string]interface{}
	yaml.Unmarshal(contentYAML, &dataYAML)
	mapstructure.Decode(dataYAML, &managerConfigYAML)
	assert.EqualValues(config, managerConfigYAML)
}
