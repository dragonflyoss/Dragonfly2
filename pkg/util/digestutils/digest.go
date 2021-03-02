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
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"

	"d7y.io/dragonfly/v2/pkg/util/fileutils"
)

// Sha256 returns the SHA-256 checksum of the data.
func Sha256(value string) string {
	h := sha256.New()
	h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}

// Sha1 returns the SHA-1 checksum of the contents.
func Sha1(contents []string) string {
	h := sha1.New()
	for _, content := range contents {
		io.WriteString(h, content)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// GetMd5Sum gets md5 sum as a string and appends the current hash to b.
func Md5SumBytes(md5 hash.Hash, b []byte) string {
	return fmt.Sprintf("%x", md5.Sum(b))
}

// Md5SumBytes generates md5 for a given file.
func Md5SumFile(name string) string {
	if !fileutils.IsRegular(name) {
		return ""
	}
	f, err := os.Open(name)
	if err != nil {
		return ""
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 8*1024*1024)
	h := md5.New()

	_, err = io.Copy(h, r)
	if err != nil {
		return ""
	}

	return Md5SumBytes(h, nil)
}
