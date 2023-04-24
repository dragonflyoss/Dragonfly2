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

	"github.com/go-redis/redis/v8"
)

func NewRedis(cfg *redis.UniversalOptions) (redis.UniversalClient, error) {
	redis.SetLogger(&redisLogger{})
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:      cfg.Addrs,
		MasterName: cfg.MasterName,
		DB:         cfg.DB,
		Username:   cfg.Username,
		Password:   cfg.Password,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return client, nil
}
