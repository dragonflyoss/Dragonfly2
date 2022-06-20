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

package digest

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/pkg/errors"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var (
	ErrDigestNotMatch = errors.New("digest not match")
)

// reader reads stream with RateLimiter.
type reader struct {
	r      io.Reader
	hash   hash.Hash
	digest string
	*logger.SugaredLoggerOnWith
}

// Reader is the interface used for reading resource.
type Reader interface {
	io.Reader
	Digest() string
}

// TODO add AF_ALG digest https://github.com/golang/sys/commit/e24f485414aeafb646f6fca458b0bf869c0880a1
func NewReader(log *logger.SugaredLoggerOnWith, r io.Reader, digest ...string) (io.Reader, error) {
	var (
		d          string
		hashMethod hash.Hash
	)
	if len(digest) > 0 {
		d = digest[0]
	}
	ds := strings.Split(d, ":")
	if len(ds) == 2 {
		d = ds[1]
		switch ds[0] {
		case "sha1":
			hashMethod = sha1.New()
		case "sha256":
			hashMethod = sha256.New()
		case "sha512":
			hashMethod = sha512.New()
		case "md5":
			hashMethod = md5.New()
		default:
			return nil, fmt.Errorf("unsupport digest method: %s", ds[0])
		}
	} else {
		hashMethod = md5.New()
	}
	return &reader{
		SugaredLoggerOnWith: log,
		digest:              d,
		hash:                hashMethod,
		r:                   r,
	}, nil
}

func (dr *reader) Read(p []byte) (int, error) {
	n, err := dr.r.Read(p)
	if err != nil && err != io.EOF {
		return n, err
	}
	if n > 0 {
		dr.hash.Write(p[:n])
	}
	if err == io.EOF && dr.digest != "" {
		digest := dr.Digest()
		if digest != dr.digest {
			dr.Warnf("digest not match, desired: %s, actual: %s", dr.digest, digest)
			return n, ErrDigestNotMatch
		}
		dr.Debugf("digest match: %s", digest)
	}
	return n, err
}

// Digest returns the digest of contents.
func (dr *reader) Digest() string {
	return hex.EncodeToString(dr.hash.Sum(nil))
}
