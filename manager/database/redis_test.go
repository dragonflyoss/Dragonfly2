/*
 *     Copyright 2023 The Dragonfly Authors
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

package database

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/manager/config"
)

func TestNewRedis(t *testing.T) {
	s, err := miniredis.Run()
	assert.Nil(t, err)
	defer s.Close()

	cfg := &config.RedisConfig{
		Addrs: []string{s.Addr()},
	}

	client, err := NewRedis(cfg)
	assert.Nil(t, err)

	pong, err := client.Ping(context.Background()).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", pong)
}
