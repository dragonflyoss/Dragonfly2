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

//go:generate mockgen -destination mocks/digest_reader_mock.go -source digest_reader.go -package mocks

package digest

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// Reader is the interface used for reading resource.
type Reader interface {
	io.Reader
	Encoded() string
}

// reader reads stream with RateLimiter.
type reader struct {
	r       io.Reader
	hash    hash.Hash
	digest  string
	encoded string
	logger  *logger.SugaredLoggerOnWith
}

// Option is a functional option for digest reader.
type Option func(reader *reader)

// WithLogger sets the logger for digest reader.
func WithLogger(logger *logger.SugaredLoggerOnWith) Option {
	return func(reader *reader) {
		reader.logger = logger
	}
}

// WithDigest sets the digest to be verified.
func WithDigest(digest string) Option {
	return func(reader *reader) {
		reader.digest = digest
	}
}

// TODO add AF_ALG digest https://github.com/golang/sys/commit/e24f485414aeafb646f6fca458b0bf869c0880a1
func NewReader(r io.Reader, options ...Option) (io.Reader, error) {
	reader := &reader{
		r:      r,
		hash:   md5.New(),
		logger: &logger.SugaredLoggerOnWith{},
	}

	for _, opt := range options {
		opt(reader)
	}

	if reader.digest != "" {
		d, err := Parse(reader.digest)
		if err != nil {
			return nil, errors.New("invalid digest")
		}

		var h hash.Hash
		switch d.Algorithm {
		case AlgorithmSHA1:
			h = sha1.New()
		case AlgorithmSHA256:
			h = sha256.New()
		case AlgorithmSHA512:
			h = sha512.New()
		case AlgorithmMD5:
			h = md5.New()
		default:
			return nil, fmt.Errorf("unsupport digest method: %s", d.Algorithm)
		}

		reader.encoded = d.Encoded
		reader.hash = h
	}

	return reader, nil
}

// Read uses to read content and validate encoded.
func (r *reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil && err != io.EOF {
		return n, err
	}

	if n > 0 {
		r.hash.Write(p[:n])
	}

	if err == io.EOF && r.digest != "" {
		encoded := r.Encoded()
		if encoded != r.encoded {
			r.logger.Warnf("digest encoded not match, desired: %s, actual: %s", r.encoded, encoded)
			return n, errors.New("digest encoded not match")
		}

		r.logger.Debugf("digest encoded match: %s", encoded)
	}

	return n, err
}

// Encoded returns the encoded of algorithm.
func (r *reader) Encoded() string {
	return hex.EncodeToString(r.hash.Sum(nil))
}
