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
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
)

const (
	// AlgorithmSHA1 is sha1 algorithm name of hash.
	AlgorithmSHA1 = "sha1"

	// AlgorithmSHA256 is sha256 algorithm name of hash.
	AlgorithmSHA256 = "sha256"

	// AlgorithmSHA512 is sha512 algorithm name of hash.
	AlgorithmSHA512 = "sha512"

	// AlgorithmMD5 is md5 algorithm name of hash.
	AlgorithmMD5 = "md5"
)

// Digest provides digest operation function.
type Digest struct {
	// Algorithm is hash algorithm.
	Algorithm string

	// Encoded is hash encode.
	Encoded string
}

// String return digest string.
func (d *Digest) String() string {
	return fmt.Sprintf("%s:%s", d.Algorithm, d.Encoded)
}

// NewDigest return digest instance.
func NewDigest(algorithm, encoded string) *Digest {
	return &Digest{
		Algorithm: algorithm,
		Encoded:   encoded,
	}
}

// HashFile computes hash value corresponding to algorithm.
func HashFile(path string, algorithm string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var h hash.Hash
	switch algorithm {
	case AlgorithmSHA1:
		h = sha1.New()
	case AlgorithmSHA256:
		h = sha256.New()
	case AlgorithmSHA512:
		h = sha512.New()
	case AlgorithmMD5:
		h = md5.New()
	default:
		return "", fmt.Errorf("unsupport digest method: %s", algorithm)
	}

	r := bufio.NewReader(f)
	_, err = io.Copy(h, r)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// Parse uses to parse digest string to algorithm and encoded.
func Parse(digest string) (*Digest, error) {
	values := strings.Split(digest, ":")
	if len(values) == 2 {
		return &Digest{
			Algorithm: values[0],
			Encoded:   values[1],
		}, nil
	}

	if len(values) == 1 {
		return &Digest{
			Algorithm: AlgorithmMD5,
			Encoded:   values[0],
		}, nil
	}

	return nil, errors.New("invalid digest")
}

// MD5FromReader computes the MD5 checksum with io.Reader.
func MD5FromReader(reader io.Reader) string {
	h := md5.New()
	r := bufio.NewReader(reader)
	if _, err := io.Copy(h, r); err != nil {
		return ""
	}

	return hex.EncodeToString(h.Sum(nil))
}

// MD5FromBytes computes the MD5 checksum with []byte.
func MD5FromBytes(bytes []byte) string {
	h := md5.New()
	h.Write(bytes)
	return hex.EncodeToString(h.Sum(nil))
}

// SHA256FromStrings computes the SHA256 checksum with multiple strings.
func SHA256FromStrings(data ...string) string {
	if len(data) == 0 {
		return ""
	}

	h := sha256.New()
	for _, s := range data {
		if _, err := h.Write([]byte(s)); err != nil {
			return ""
		}
	}

	return hex.EncodeToString(h.Sum(nil))
}
