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
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"

	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/pkg/errors"
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

func Md5File(name string) string {
	if !fileutils.IsRegular(name) {
		return ""
	}

	f, err := os.Open(name)
	if err != nil {
		return ""
	}

	defer f.Close()

	h := md5.New()

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

func GenerateRandomSalt(saltSize int) (string, error) {
	if saltSize <= 0 {
		return "", errors.New("saltSize should be a positive number")
	}
	salt := make([]byte, saltSize)

	_, err := rand.Read(salt[:])

	if err != nil {
		return "", err
	}

	return string(salt), nil

}
