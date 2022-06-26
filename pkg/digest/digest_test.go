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
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/basic"
)

func TestSha256(t *testing.T) {
	var expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	assert.Equal(t, expected, SHA256FromStrings("hello"))
}

func TestMd5Bytes(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"
	assert.Equal(t, expected, MD5FromBytes([]byte("hello")))
}

func TestToHashString(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"
	h := md5.New()
	h.Write([]byte("hello"))
	assert.Equal(t, expected, hex.EncodeToString(h.Sum(nil)))
}

func TestMd5Reader(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"
	assert.Equal(t, expected, MD5FromReader(strings.NewReader("hello")))
}

func TestHashFile(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"
	path := filepath.Join(basic.TmpDir, uuid.NewString())
	f, err := os.OpenFile(path, syscall.O_CREAT|syscall.O_TRUNC|syscall.O_RDWR, fs.FileMode(0644))
	assert.Nil(t, err)
	defer f.Close()

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	encoded, err := HashFile(path, AlgorithmMD5)
	assert.NoError(t, err)
	assert.Equal(t, expected, encoded)
}
