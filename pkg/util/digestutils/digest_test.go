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
	"crypto/md5"
	"strings"
	"syscall"
	"testing"

	"d7y.io/dragonfly/v2/pkg/basic"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSha256(t *testing.T) {
	var expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	assert.Equal(t, expected, Sha256("hello"))
}

func TestMd5Bytes(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"
	assert.Equal(t, expected, Md5Bytes([]byte("hello")))
}

func TestToHashString(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"
	h := md5.New()
	h.Write([]byte("hello"))
	assert.Equal(t, expected, ToHashString(h))
}

func TestMd5Reader(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"
	assert.Equal(t, expected, Md5Reader(strings.NewReader("hello")))
}

func TestMd5File(t *testing.T) {
	var expected = "5d41402abc4b2a76b9719d911017c592"

	path := basic.TmpDir + "/" + uuid.New().String()
	f, err := fileutils.OpenFile(path, syscall.O_CREAT|syscall.O_TRUNC|syscall.O_RDWR, 0644)
	assert.Nil(t, err)

	f.Write([]byte("hello"))
	f.Close()

	assert.Equal(t, expected, Md5File(path))
}
