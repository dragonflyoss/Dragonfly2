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
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"strings"

	"github.com/opencontainers/go-digest"

	"d7y.io/dragonfly/v2/pkg/unit"
)

const (
	Sha256Hash digest.Algorithm = "sha256"
	Md5Hash    digest.Algorithm = "md5"
)

var (
	// Algorithms is used to check if an algorithm is supported.
	// If algo is not supported, Algorithms[algo] will return empty string.
	// Please don't use digest.Algorithm() to convert a string to digest.Algorithm.
	Algorithms = map[string]digest.Algorithm{
		Sha256Hash.String(): Sha256Hash,
		Md5Hash.String():    Md5Hash,
	}
)

func Sha256(values ...string) string {
	if len(values) == 0 {
		return ""
	}

	h := sha256.New()
	for _, content := range values {
		if _, err := h.Write([]byte(content)); err != nil {
			return ""
		}
	}

	return ToHashString(h)
}

func Md5Reader(reader io.Reader) string {
	h := md5.New()
	if _, err := io.Copy(h, reader); err != nil {
		return ""
	}

	return ToHashString(h)
}

func Md5Bytes(bytes []byte) string {
	h := md5.New()
	h.Write(bytes)
	return ToHashString(h)
}

// HashFile computes hash value corresponding to hashType,
// hashType is from digestutils.Md5Hash and digestutils.Sha256Hash.
func HashFile(path string, hashType digest.Algorithm) string {
	file, err := os.Stat(path)
	if err != nil {
		return ""
	}

	if !file.Mode().IsRegular() {
		return ""
	}

	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	var h hash.Hash
	if hashType == Md5Hash {
		h = md5.New()
	} else if hashType == Sha256Hash {
		h = sha256.New()
	} else {
		return ""
	}

	r := bufio.NewReaderSize(f, int(4*unit.MB))

	_, err = io.Copy(h, r)
	if err != nil {
		return ""
	}

	return ToHashString(h)
}

func ToHashString(h hash.Hash) string {
	return hex.EncodeToString(h.Sum(nil))
}

func Parse(digest string) []string {
	digest = strings.Trim(digest, " ")
	return strings.Split(digest, ":")
}

func CreateHash(hashType string) hash.Hash {
	algo := Algorithms[hashType]
	switch algo {
	case Sha256Hash:
		return sha256.New()
	case Md5Hash:
		return md5.New()
	default:
		return nil
	}
}
