package clientutil

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"testing"

	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	testifyassert "github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	logcore.InitDaemon()
	m.Run()
}

func TestNewDigestReader(t *testing.T) {
	assert := testifyassert.New(t)

	testBytes := []byte("hello world")
	hash := md5.New()
	hash.Write(testBytes)
	digest := hex.EncodeToString(hash.Sum(nil)[:16])

	buf := bytes.NewBuffer(testBytes)
	reader := NewDigestReader(buf, digest)
	data, err := ioutil.ReadAll(reader)

	assert.Nil(err)
	assert.Equal(testBytes, data)
}
