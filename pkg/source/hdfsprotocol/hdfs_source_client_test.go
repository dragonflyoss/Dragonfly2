package hdfsprotocol

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/source"
	"github.com/colinmarc/hdfs/v2"
	"github.com/go-http-utils/headers"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

var sourceClient = NewHDFSSourceClient()

const (
	hdfsExistFileHost                     = "hdfs://127.0.0.1:9000"
	hdfsExistFilePath                     = "/user/root/input/f1.txt"
	hdfsExistFileUrl                      = hdfsExistFileHost + hdfsExistFilePath
	hdfsExistFileContentLength      int64 = 12
	hdfsExistFileContent                  = "Hello World\n"
	hdfsExistFileLastModifiedMillis int64 = 1625189350085
	hdfsExistFileRangeContent             = "World\n"
	hdfsExistFileLastModified             = "2021-07-02 09:29:10"
)

const (
	hdfsNotExistFileUrl                 = "hdfs://127.0.0.1:9000/user/root/input/f3.txt"
	hdfsNotExistFileContentLength int64 = -1
)

// TestGetContentLength function test exist and not exist file
func TestGetContentLength(t *testing.T) {
	// exist file
	length, err := sourceClient.GetContentLength(context.Background(), hdfsExistFileUrl, nil)
	assert.Equal(t, hdfsExistFileContentLength, length)
	assert.Nil(t, err)

	// not exist file
	length, err = sourceClient.GetContentLength(context.Background(), hdfsNotExistFileUrl, nil)
	assert.Equal(t, hdfsNotExistFileContentLength, length)
	assert.EqualError(t, err, "stat /user/root/input/f3.txt: file does not exist")

}

func TestIsSupportRange(t *testing.T) {
	supportRange, err := sourceClient.IsSupportRange(context.Background(), hdfsExistFileUrl, nil)
	assert.Equal(t, true, supportRange)
	assert.Nil(t, err)
}

func TestIsExpired(t *testing.T) {
	// header not have Last-Modified
	expired, err := sourceClient.IsExpired(context.Background(), hdfsExistFileUrl, nil, map[string]string{})
	assert.Equal(t, true, expired)
	assert.Nil(t, err)

	// header have Last-Modified
	expired, err = sourceClient.IsExpired(context.Background(), hdfsExistFileUrl, nil, map[string]string{
		headers.LastModified: "2020-01-01 00:00:00",
	})
	assert.Equal(t, true, expired)
	assert.Nil(t, err)

	// header have Last-Modified
	expired, err = sourceClient.IsExpired(context.Background(), hdfsExistFileUrl, nil, map[string]string{
		headers.LastModified: hdfsExistFileLastModified,
	})
	assert.Equal(t, false, expired)
	assert.Nil(t, err)
}

func TestDownload(t *testing.T) {
	// exist file
	download, err := sourceClient.Download(context.Background(), hdfsExistFileUrl, nil)
	data, err := ioutil.ReadAll(download)

	assert.Equal(t, hdfsExistFileContent, string(data))
	assert.Nil(t, err)

	// not exist file
	download, err = sourceClient.Download(context.Background(), hdfsNotExistFileUrl, nil)
	assert.Nil(t, download)
	assert.Error(t, err)
}

func TestDownloadWithResponseHeader(t *testing.T) {
	body, responseHeader, err := sourceClient.DownloadWithResponseHeader(context.Background(), hdfsExistFileUrl, map[string]string{
		"Range": "6",
	})
	assert.Nil(t, err)
	assert.Equal(t, hdfsExistFileLastModified, responseHeader.Get(source.LastModified))

	data, _ := ioutil.ReadAll(body)
	assert.Equal(t, hdfsExistFileRangeContent, string(data))
}

func TestGetLastModifiedMillis(t *testing.T) {
	lastModifiedMillis, err := sourceClient.GetLastModifiedMillis(context.Background(), hdfsExistFileUrl, nil)
	assert.Nil(t, err)
	assert.Equal(t, hdfsExistFileLastModifiedMillis, lastModifiedMillis)
}

func TestNewHDFSSourceClient(t *testing.T) {
	client := NewHDFSSourceClient()
	assert.NotNil(t, client)

	options := make([]HDFSSourceClientOption, 0)

	option := func(p *hdfsSourceClient) {
		c, _ := hdfs.New(hdfsExistFileHost)
		p.clientMap[hdfsExistFileHost] = c
	}
	options = append(options, option)

	newHDFSSourceClient := NewHDFSSourceClient(options...)

	assert.IsType(t, &hdfsSourceClient{}, newHDFSSourceClient)

}
