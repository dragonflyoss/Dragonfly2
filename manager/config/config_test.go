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
	"os"
	"testing"

	"github.com/mitchellh/mapstructure"
	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"d7y.io/dragonfly/v2/pkg/objectstorage"
)

func TestManagerConfig_Load(t *testing.T) {
	assert := testifyassert.New(t)

	config := &Config{
		Server: &ServerConfig{
			Name:     "foo",
			CacheDir: "foo",
			LogDir:   "foo",
			GRPC: &TCPListenConfig{
				ListenIP: "0.0.0.0",
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
			Type: "mysql",
			Mysql: &MysqlConfig{
				User:      "foo",
				Password:  "foo",
				Host:      "foo",
				Port:      3306,
				DBName:    "foo",
				TLSConfig: "preferred",
				TLS: &TLSConfig{
					Cert:               "foo",
					Key:                "foo",
					CA:                 "foo",
					InsecureSkipVerify: true,
				},
				Migrate: true,
			},
			Postgres: &PostgresConfig{
				User:     "foo",
				Password: "foo",
				Host:     "foo",
				Port:     5432,
				DBName:   "foo",
				SSLMode:  "disable",
				Timezone: "UTC",
				Migrate:  true,
			},
			Redis: &RedisConfig{
				Host:      "bar",
				Password:  "bar",
				Addrs:     []string{"foo", "bar"},
				Port:      6379,
				DB:        0,
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
		ObjectStorage: &ObjectStorageConfig{
			Enable:    true,
			Name:      objectstorage.ServiceNameS3,
			Endpoint:  "127.0.0.1",
			AccessKey: "foo",
			SecretKey: "bar",
			Region:    "baz",
		},
		Security: &SecurityConfig{
			AutoIssueCert: true,
			CACert:        "foo",
			CAKey:         "bar",
			TLSPolicy:     "force",
			CertSpec: &CertSpec{
				IPAddresses:    []string{"127.0.0.1"},
				DNSNames:       []string{"foo"},
				ValidityPeriod: 1000,
			},
		},
		Metrics: &MetricsConfig{
			Enable:          true,
			Addr:            ":8000",
			EnablePeerGauge: false,
		},
	}

	managerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/manager.yaml")
	var dataYAML map[string]any
	if err := yaml.Unmarshal(contentYAML, &dataYAML); err != nil {
		t.Fatal(err)
	}

	if err := mapstructure.Decode(dataYAML, &managerConfigYAML); err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(config, managerConfigYAML)
}
