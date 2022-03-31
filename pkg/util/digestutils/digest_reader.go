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

package digestutils

import (
	"hash"
	"io"
	"strings"

	"github.com/pkg/errors"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var (
	ErrDigestNotMatch = errors.New("digest not match")
)

// digestReader reads stream with digest.
type digestReader struct {
	r         io.Reader
	hash      hash.Hash
	hexDigest string
	*logger.SugaredLoggerOnWith
}

type DigestReader interface {
	io.Reader
	Digest() string
}

// TODO add AF_ALG digest https://github.com/golang/sys/commit/e24f485414aeafb646f6fca458b0bf869c0880a1

// NewDigestReader digest format is <algorithm>:<hex_digest_string>. If digest is not specified, use default md5
func NewDigestReader(log *logger.SugaredLoggerOnWith, reader io.Reader, digest ...string) (io.Reader, error) {
	var hexDigest string
	hashType := Md5Hash.String()
	if len(digest) > 1 {
		return nil, errors.Errorf("only zero or one number digest can be specified, but got %d", len(digest))
	}
	if len(digest) == 1 && digest[0] != "" {
		if strings.Contains(digest[0], ":") {
			parsedHash := Parse(digest[0])
			if len(parsedHash) != 2 {
				return nil, errors.Errorf("malformed digest format, expect  <algorithm>:<hex_digest_string>, but got %s", digest[0])
			}
			hashType = parsedHash[0]
			hexDigest = parsedHash[1]
		} else {
			hashType = Md5Hash.String()
			hexDigest = digest[0]
		}
	}
	hash := CreateHash(hashType)
	if hash == nil {
		return nil, errors.Errorf("digest algorithm %s is not support", hashType)
	}
	return &digestReader{
		SugaredLoggerOnWith: log,
		hexDigest:           hexDigest,
		hash:                hash,
		r:                   reader,
	}, nil
}

func (dr *digestReader) Read(p []byte) (int, error) {
	n, err := dr.r.Read(p)
	if err != nil && err != io.EOF {
		return n, err
	}
	if n > 0 {
		dr.hash.Write(p[:n])
	}
	if err == io.EOF && dr.hexDigest != "" {
		digest := dr.Digest()
		if digest != dr.hexDigest {
			dr.Warnf("digest not match, desired: %s, actual: %s", dr.hexDigest, digest)
			return n, ErrDigestNotMatch
		}
		dr.Debugf("digest match: %s", digest)
	}
	return n, err
}

// Digest returns the digest of contents.
func (dr *digestReader) Digest() string {
	return ToHashString(dr.hash)
}
