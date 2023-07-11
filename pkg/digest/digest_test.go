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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDigest_String(t *testing.T) {
	assert.Equal(t, New(AlgorithmMD5, "5d41402abc4b2a76b9719d911017c592").String(), "md5:5d41402abc4b2a76b9719d911017c592")
}

func TestDigest_HashFile(t *testing.T) {
	path := filepath.Join(os.TempDir(), uuid.NewString())
	f, err := os.OpenFile(path, syscall.O_CREAT|syscall.O_TRUNC|syscall.O_RDWR, fs.FileMode(0644))
	assert.Nil(t, err)
	defer f.Close()

	tests := []struct {
		algorithm string
		encoded   string
	}{
		{AlgorithmSHA1, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"},
		{AlgorithmSHA256, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		{AlgorithmSHA512, "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"},
		{AlgorithmMD5, "5d41402abc4b2a76b9719d911017c592"},
	}

	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	for _, tc := range tests {
		encoded, err := HashFile(path, tc.algorithm)
		assert.NoError(t, err)
		assert.Equal(t, tc.encoded, encoded)
	}
}

func TestDigest_Parse(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		expect func(t *testing.T, digest *Digest, err error)
	}{
		{
			name:  "sha1 digest",
			value: "sha1:7df059597099bb7dcf25d2a9aedfaf4465f72d8d",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(d, New(AlgorithmSHA1, "7df059597099bb7dcf25d2a9aedfaf4465f72d8d"))
			},
		},
		{
			name:  "invalid sha1 encoded",
			value: "sha1:7df059597099bb7dcf25d2a9aedfaf4465f72d8",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name:  "sha256 digest",
			value: "sha256:c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(d, New(AlgorithmSHA256, "c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"))
			},
		},
		{
			name:  "invalid sha256 encoded",
			value: "sha256:c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name:  "sha512 digest",
			value: "sha512:dc6b68d13b8cf959644b935f1192b02c71aa7a5cf653bd43b4480fa89eec8d4d3f16a2278ec8c3b40ab1fdb233b3173a78fd83590d6f739e0c9e8ff56c282557",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(d, New(AlgorithmSHA512, "dc6b68d13b8cf959644b935f1192b02c71aa7a5cf653bd43b4480fa89eec8d4d3f16a2278ec8c3b40ab1fdb233b3173a78fd83590d6f739e0c9e8ff56c282557"))
			},
		},
		{
			name:  "invalid sha512 encoded",
			value: "sha512:dc6b68d13b8cf959644b935f1192b02c71aa7a5cf653bd43b4480fa89eec8d4d3f16a2278ec8c3b40ab1fdb233b3173a78fd83590d6f739e0c9e8ff56c28255",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name:  "md5 digest",
			value: "md5:5d41402abc4b2a76b9719d911017c592",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.EqualValues(d, New(AlgorithmMD5, "5d41402abc4b2a76b9719d911017c592"))
			},
		},
		{
			name:  "invalid md5 encoded",
			value: "md5:5d41402abc4b2a76b9719d911017c59",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name:  "invalid algorithm",
			value: "foo:5d41402abc4b2a76b9719d911017c592",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
		{
			name:  "invalid digest",
			value: "bar",
			expect: func(t *testing.T, d *Digest, err error) {
				assert := assert.New(t)
				assert.Error(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := Parse(tc.value)

			tc.expect(t, d, err)
		})
	}
}

func TestDigest_MD5FromReader(t *testing.T) {
	assert.Equal(t, "5d41402abc4b2a76b9719d911017c592", MD5FromReader(strings.NewReader("hello")))
}

func TestDigest_MD5FromBytes(t *testing.T) {
	assert.Equal(t, "5d41402abc4b2a76b9719d911017c592", MD5FromBytes([]byte("hello")))
}

func TestDigest_SHA256FromStrings(t *testing.T) {
	assert.Equal(t, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", SHA256FromStrings("hello"))
}

func TestDigest_SHA256FromBytes(t *testing.T) {
	assert.Equal(t, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", SHA256FromBytes([]byte("hello")))
}
