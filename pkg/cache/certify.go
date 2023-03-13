/*
 *     Copyright 2022 The Dragonfly Authors
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

package cache

import (
	"context"
	"crypto/tls"

	"github.com/johanbrandhorst/certify"
)

const (
	// CertifyCacheDirName is dir name of certify cache.
	CertifyCacheDirName = "certs"
)

type certifyCache struct {
	caches []certify.Cache
}

// NewCertifyMutliCache returns a certify.Cache with multiple caches
// Such as, cache.NewCertifyMutliCache(certify.NewMemCache(), certify.DirCache("certs"))
// This multiple cache will get certs from mem cache first, then dir cache to avoid read from filesystem every times.
func NewCertifyMutliCache(caches ...certify.Cache) certify.Cache {
	return &certifyCache{caches: caches}
}

// Get gets cert from cache one by one, if found, puts it back to all previous caches.
func (c *certifyCache) Get(ctx context.Context, key string) (cert *tls.Certificate, err error) {
	var foundCacheIdx int = -1
	for i, cache := range c.caches {
		if cert, err = cache.Get(ctx, key); err == nil {
			foundCacheIdx = i
			break
		}
	}

	// put cert in previous cache
	for i := 0; i < foundCacheIdx; i++ {
		_ = c.caches[i].Put(ctx, key, cert)
	}

	if err == nil {
		return
	}

	return nil, certify.ErrCacheMiss
}

// Put puts cert to all caches.
func (c *certifyCache) Put(ctx context.Context, key string, cert *tls.Certificate) error {
	for _, cache := range c.caches {
		if err := cache.Put(ctx, key, cert); err != nil {
			return err
		}
	}
	return nil
}

// Delete deletes cert from all caches.
func (c *certifyCache) Delete(ctx context.Context, key string) error {
	for _, cache := range c.caches {
		if err := cache.Delete(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
