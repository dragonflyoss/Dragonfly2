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
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestNewDigestReader(t *testing.T) {
	assert := testifyassert.New(t)

	testBytes := []byte("hello world")
	hash := md5.New()
	hash.Write(testBytes)
	digest := hex.EncodeToString(hash.Sum(nil)[:16])

	buf := bytes.NewBuffer(testBytes)
	reader := NewDigestReader(logger.With("test", "test"), buf, digest)
	data, err := io.ReadAll(reader)

	assert.Nil(err)
	assert.Equal(testBytes, data)
}
