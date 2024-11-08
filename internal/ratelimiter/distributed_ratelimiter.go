/*
 *     Copyright 2024 The Dragonfly Authors
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

package ratelimiter

import (
	"context"
	"time"

	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/mennanov/limiters"
	redis "github.com/redis/go-redis/v9"

	pkgredis "d7y.io/dragonfly/v2/pkg/redis"
)

// DistributedRateLimiter is an interface for a distributed rate limiter.
type DistributedRateLimiter interface {
	// TokenBucket returns a token bucket rate limiter.
	TokenBucket(ctx context.Context, capacity int64, per time.Duration) *limiters.TokenBucket
}

// distributedRateLimiter is an implementation of DistributedRateLimiter.
type distributedRateLimiter struct {
	// Key used to store the rate limit in the database.
	key string

	// Database used to store the rate limit.
	rdb redis.UniversalClient
}

// NewDistributedRateLimiter creates a new instance of DistributedRateLimiter.
func NewDistributedRateLimiter(rdb redis.UniversalClient, key string) DistributedRateLimiter {
	return &distributedRateLimiter{key, rdb}
}

// locker returns a distributed locker.
func (d *distributedRateLimiter) locker() limiters.DistLocker {
	return limiters.NewLockRedis(goredis.NewPool(d.rdb), pkgredis.MakeDistributedRateLimiterLockerKeyInManager(d.key))
}

// TokenBucket returns a token bucket rate limiter.
func (d *distributedRateLimiter) TokenBucket(ctx context.Context, capacity int64, per time.Duration) *limiters.TokenBucket {
	bucket := limiters.NewTokenBucketRedis(d.rdb, pkgredis.MakeDistributedRateLimiterKeyInManager(d.key), per, false)
	return limiters.NewTokenBucket(capacity, per, d.locker(), bucket, limiters.NewSystemClock(), limiters.NewStdLogger())
}
