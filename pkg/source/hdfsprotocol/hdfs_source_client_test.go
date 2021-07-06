package hdfsprotocol

import (
	"context"
	"io"
	"io/fs"
	"io/ioutil"
	"reflect"
	"strconv"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/pkg/source"
	"github.com/agiledragon/gomonkey"
	"github.com/colinmarc/hdfs/v2"
	"github.com/go-http-utils/headers"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var sourceClient source.ResourceClient

const (
	hdfsExistFileHost                     = "127.0.0.1:9000"
	hdfsExistFilePath                     = "/user/root/input/f1.txt"
	hdfsExistFileURL                      = "hdfs://" + hdfsExistFileHost + hdfsExistFilePath
	hdfsExistFileContentLength      int64 = 12
	hdfsExistFileContent                  = "Hello World\n"
	hdfsExistFileLastModifiedMillis int64 = 1625218150000
	hdfsExistFileRangeContent             = "World\n"
	hdfsExistFileLastModified             = "2021-07-02 09:29:10"
	hdfsExistFileRange              int   = 6
)

const (
	hdfsNotExistFileURL                 = "hdfs://127.0.0.1:9000/user/root/input/f3.txt"
	hdfsNotExistFileContentLength int64 = -1
)

var fakeHDFSClient *hdfs.Client = &hdfs.Client{}

func testBefore() {
	sourceClient = NewHDFSSourceClient(func(p *hdfsSourceClient) {
		p.clientMap[hdfsExistFileHost] = fakeHDFSClient
	})
}

func TestMain(t *testing.M) {
	testBefore()
	t.Run()
}

// TestGetContentLength function test exist and not exist file
func TestGetContentLength_OK(t *testing.T) {

	var info fs.FileInfo = fakeHDFSFileInfo{
		contents: hdfsExistFileContent,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	// exist file
	length, err := sourceClient.GetContentLength(context.Background(), hdfsExistFileURL, nil)
	assert.Equal(t, hdfsExistFileContentLength, length)
	assert.Nil(t, err)

}

func TestGetContentLength_Fail(t *testing.T) {

	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("stat /user/root/input/f3.txt: file does not exist")}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	// not exist file
	length, err := sourceClient.GetContentLength(context.Background(), hdfsNotExistFileURL, nil)
	assert.Equal(t, hdfsNotExistFileContentLength, length)
	assert.EqualError(t, err, "stat /user/root/input/f3.txt: file does not exist")
}

func TestIsSupportRange(t *testing.T) {
	supportRange, err := sourceClient.IsSupportRange(context.Background(), hdfsExistFileURL, nil)
	assert.Equal(t, true, supportRange)
	assert.Nil(t, err)
}

func TestIsExpired_NoHeader(t *testing.T) {
	// header not have Last-Modified
	expired, err := sourceClient.IsExpired(context.Background(), hdfsExistFileURL, nil, map[string]string{})
	assert.Equal(t, true, expired)
	assert.Nil(t, err)
}
func TestIsExpired_LastModifiedExpired(t *testing.T) {
	lastModified, _ := time.Parse(layout, hdfsExistFileLastModified)
	var info fs.FileInfo = fakeHDFSFileInfo{
		modtime: lastModified,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	// header have Last-Modified
	expired, err := sourceClient.IsExpired(context.Background(), hdfsExistFileURL, nil, map[string]string{
		headers.LastModified: "2020-01-01 00:00:00",
	})
	assert.Equal(t, true, expired)
	assert.Nil(t, err)

}

func TestIsExpired_LastModifiedNotExpired(t *testing.T) {
	lastModified, _ := time.Parse(layout, hdfsExistFileLastModified)
	var info fs.FileInfo = fakeHDFSFileInfo{
		modtime: lastModified,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	// header have Last-Modified
	expired, err := sourceClient.IsExpired(context.Background(), hdfsExistFileURL, nil, map[string]string{
		headers.LastModified: hdfsExistFileLastModified,
	})
	assert.Equal(t, false, expired)
	assert.Nil(t, err)
}

func TestDownload_FileExist(t *testing.T) {
	var reader *hdfs.FileReader = &hdfs.FileReader{}
	patch := gomonkey.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return reader, nil
	})
	patch.ApplyMethod(reflect.TypeOf(reader), "Read", func(_ *hdfs.FileReader, b []byte) (int, error) {
		byets := []byte(hdfsExistFileContent)
		copy(b, byets)
		//for i, byet := range byets {
		//	b[i] = byet
		//}
		return int(hdfsExistFileContentLength), io.EOF
	})

	defer patch.Reset()

	// exist file
	download, err := sourceClient.Download(context.Background(), hdfsExistFileURL, nil)
	data, _ := ioutil.ReadAll(download)

	assert.Equal(t, hdfsExistFileContent, string(data))
	assert.Nil(t, err)

}

func TestDownload_FileNotExist(t *testing.T) {
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("open /user/root/input/f3.txt: file does not exist")}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Open", stubRet)

	defer patch.Reset()
	// not exist file
	download, err := sourceClient.Download(context.Background(), hdfsNotExistFileURL, nil)
	assert.Nil(t, download)
	assert.EqualError(t, err, "open /user/root/input/f3.txt: file does not exist")
}

func TestDownloadWithResponseHeader_FileExist(t *testing.T) {
	lastModified, _ := time.Parse(layout, hdfsExistFileLastModified)
	var reader *hdfs.FileReader = &hdfs.FileReader{}
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return reader, nil
	})
	patches.ApplyMethodSeq(reflect.TypeOf(reader), "Stat", []gomonkey.OutputCell{
		{
			Values: gomonkey.Params{
				fakeHDFSFileInfo{
					contents: hdfsExistFileContent,
					modtime:  lastModified,
				},
			},
		},
	})

	patches.ApplyMethod(reflect.TypeOf(reader), "Seek", func(_ *hdfs.FileReader, offset int64, whence int) (int64, error) {
		return hdfsExistFileContentLength - int64(hdfsExistFileRange), nil
	})
	patches.ApplyMethod(reflect.TypeOf(reader), "Read", func(_ *hdfs.FileReader, b []byte) (int, error) {
		byets := []byte(hdfsExistFileContent)
		var j int = 0
		for i, byet := range byets {
			if i < hdfsExistFileRange {
				i++
				continue
			}
			b[j] = byet
			j++
		}
		return len(b), nil
	})

	body, responseHeader, err := sourceClient.DownloadWithResponseHeader(context.Background(), hdfsExistFileURL, map[string]string{
		"Range": strconv.Itoa(hdfsExistFileRange),
	})
	assert.Nil(t, err)
	assert.Equal(t, hdfsExistFileLastModified, responseHeader.Get(source.LastModified))

	data, _ := ioutil.ReadAll(body)
	assert.Equal(t, hdfsExistFileRangeContent, string(data))
}

func TestDownloadWithResponseHeader_FileNotExist(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return nil, errors.New("open /user/root/input/f3.txt: file does not exist")
	})
	defer patch.Reset()

	body, responseHeader, err := sourceClient.DownloadWithResponseHeader(context.Background(), hdfsExistFileURL, map[string]string{
		"Range": "6",
	})
	assert.EqualError(t, err, "open /user/root/input/f3.txt: file does not exist")
	assert.Nil(t, responseHeader)
	assert.Nil(t, body)
}

func TestGetLastModifiedMillis_FileExist(t *testing.T) {
	lastModified, _ := time.Parse(layout, hdfsExistFileLastModified)
	var info fs.FileInfo = fakeHDFSFileInfo{
		modtime: lastModified,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	lastModifiedMillis, err := sourceClient.GetLastModifiedMillis(context.Background(), hdfsExistFileURL, nil)
	assert.Nil(t, err)
	assert.Equal(t, hdfsExistFileLastModifiedMillis, lastModifiedMillis)
}

func TestGetLastModifiedMillis_FileNotExist(t *testing.T) {
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("stat /user/root/input/f3.txt: file does not exist")}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	lastModifiedMillis, err := sourceClient.GetLastModifiedMillis(context.Background(), hdfsNotExistFileURL, nil)
	assert.EqualError(t, err, "stat /user/root/input/f3.txt: file does not exist")
	assert.Equal(t, hdfsNotExistFileContentLength, lastModifiedMillis)
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

type fakeHDFSFileInfo struct {
	dir      bool
	basename string
	modtime  time.Time
	contents string
}

func (f fakeHDFSFileInfo) Name() string       { return f.basename }
func (f fakeHDFSFileInfo) Sys() interface{}   { return nil }
func (f fakeHDFSFileInfo) ModTime() time.Time { return f.modtime }
func (f fakeHDFSFileInfo) IsDir() bool        { return f.dir }
func (f fakeHDFSFileInfo) Size() int64        { return int64(len(f.contents)) }
func (f fakeHDFSFileInfo) Mode() fs.FileMode {
	if f.dir {
		return 0755 | fs.ModeDir
	}
	return 0644
}
