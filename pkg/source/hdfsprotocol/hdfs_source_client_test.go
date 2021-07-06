package hdfsprotocol

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/cdnsystem/daemon/cdn"
	"d7y.io/dragonfly/v2/pkg/util/rangeutils"

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
	hdfsExistFileLastModified             = "2021-07-02 09:29:10"
	hdfsExistFileRangeStart         int64 = 3
	hdfsExistFileRangeEnd           int64 = 10
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
	os.Exit(0)
}

// TestGetContentLength_OK function test exist file and return file length
func TestGetContentLength_OK(t *testing.T) {

	var info os.FileInfo = fakeHDFSFileInfo{
		contents: hdfsExistFileContent,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	// exist file
	length, err := sourceClient.GetContentLength(context.Background(), hdfsExistFileURL, nil, &rangeutils.Range{0, 12})
	assert.Equal(t, hdfsExistFileContentLength, length)
	assert.Nil(t, err)

}

// TestGetContentLength_Fail test file not exist, return error
func TestGetContentLength_Fail(t *testing.T) {

	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("stat /user/root/input/f3.txt: file does not exist")}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	// not exist file
	length, err := sourceClient.GetContentLength(context.Background(), hdfsNotExistFileURL, nil, &rangeutils.Range{0, 10})
	assert.Equal(t, hdfsNotExistFileContentLength, length)
	assert.EqualError(t, err, "stat /user/root/input/f3.txt: file does not exist")
}

// TestIsSupportRange_FileExist test file exist, return file  support range
func TestIsSupportRange_FileExist(t *testing.T) {
	var info os.FileInfo = fakeHDFSFileInfo{
		contents: hdfsExistFileContent,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	supportRange, err := sourceClient.IsSupportRange(context.Background(), hdfsExistFileURL, nil)
	assert.Equal(t, true, supportRange)
	assert.Nil(t, err)
}

// TestIsSupportRange_FileNotExist test file not exist, return error and not support range
func TestIsSupportRange_FileNotExist(t *testing.T) {
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("stat /user/root/input/f3.txt: file does not exist")}}, // 模拟第一次调用Delete的时候，删除成功，返回nil
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	supportRange, err := sourceClient.IsSupportRange(context.Background(), hdfsNotExistFileURL, nil)
	assert.Equal(t, false, supportRange)
	assert.EqualError(t, err, "stat /user/root/input/f3.txt: file does not exist")
}

//
func TestIsExpired_NoHeader(t *testing.T) {
	// header not have Last-Modified
	expired, err := sourceClient.IsExpired(context.Background(), hdfsExistFileURL, nil, map[string]string{})
	assert.Equal(t, true, expired)
	assert.Nil(t, err)
}
func TestIsExpired_LastModifiedExpired(t *testing.T) {
	lastModified, _ := time.Parse(layout, hdfsExistFileLastModified)
	var info os.FileInfo = fakeHDFSFileInfo{
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
	var info os.FileInfo = fakeHDFSFileInfo{
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

func Test_Download_FileExist_ByRang(t *testing.T) {
	var reader *hdfs.FileReader = &hdfs.FileReader{}
	patch := gomonkey.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return reader, nil
	})
	patch.ApplyMethod(reflect.TypeOf(reader), "Seek", func(_ *hdfs.FileReader, offset int64, whence int) (int64, error) {
		return 0 - hdfsExistFileContentLength, nil
	})
	patch.ApplyMethod(reflect.TypeOf(reader), "Read", func(_ *hdfs.FileReader, b []byte) (int, error) {
		byets := []byte(hdfsExistFileContent)
		copy(b, byets)
		return int(hdfsExistFileContentLength), io.EOF
	})
	patch.ApplyMethodSeq(reflect.TypeOf(reader), "Stat", []gomonkey.OutputCell{
		{
			Values: gomonkey.Params{
				fakeHDFSFileInfo{
					contents: hdfsExistFileContent,
				},
			},
		},
	})
	defer patch.Reset()

	rang := &rangeutils.Range{StartIndex: 0, EndIndex: uint64(hdfsExistFileContentLength)}
	// exist file
	download, err := sourceClient.Download(context.Background(), hdfsExistFileURL, source.RequestHeader{
		cdn.RangeHeaderName: fmt.Sprintf("bytes=%s", rang.String()),
	}, rang)
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

	rang := rangeutils.Range{StartIndex: 0, EndIndex: uint64(hdfsExistFileContentLength)}
	// not exist file
	download, err := sourceClient.Download(context.Background(), hdfsNotExistFileURL, source.RequestHeader{
		cdn.RangeHeaderName: fmt.Sprintf("bytes=%s", rang.String()),
	}, &rang)
	assert.Nil(t, download)
	assert.EqualError(t, err, "open /user/root/input/f3.txt: file does not exist")
}

func Test_DownloadWithResponseHeader_FileExist_ByRange(t *testing.T) {
	lastModified, _ := time.Parse(layout, hdfsExistFileLastModified)
	var reader *hdfs.FileReader = &hdfs.FileReader{}
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	patches.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return reader, nil
	})
	patches.ApplyMethod(reflect.TypeOf(reader), "Stat", func(_ *hdfs.FileReader) os.FileInfo {
		return fakeHDFSFileInfo{
			contents: hdfsExistFileContent,
			modtime:  lastModified,
		}
	})

	patches.ApplyMethod(reflect.TypeOf(reader), "Seek", func(_ *hdfs.FileReader, offset int64, whence int) (int64, error) {
		return hdfsExistFileRangeEnd - hdfsExistFileRangeStart, nil
	})
	patches.ApplyMethod(reflect.TypeOf(reader), "Read", func(_ *hdfs.FileReader, b []byte) (int, error) {
		b = b[0 : hdfsExistFileRangeEnd-hdfsExistFileRangeStart]
		bytes := []byte(hdfsExistFileContent)
		copy(b, bytes[hdfsExistFileRangeStart:hdfsExistFileRangeEnd])
		return len(b), io.EOF
	})

	rang := rangeutils.Range{StartIndex: uint64(hdfsExistFileRangeStart), EndIndex: uint64(hdfsExistFileRangeEnd)}
	body, responseHeader, err := sourceClient.DownloadWithResponseHeader(context.Background(), hdfsExistFileURL, source.RequestHeader{
		cdn.RangeHeaderName: fmt.Sprintf("bytes=%s", rang.String()),
	}, &rang)
	assert.Nil(t, err)
	assert.Equal(t, hdfsExistFileLastModified, responseHeader.Get(source.LastModified))

	data, _ := ioutil.ReadAll(body)
	assert.Equal(t, string(data), string([]byte(hdfsExistFileContent)[hdfsExistFileRangeStart:hdfsExistFileRangeEnd]))
}

func TestDownloadWithResponseHeader_FileNotExist(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return nil, errors.New("open /user/root/input/f3.txt: file does not exist")
	})
	defer patch.Reset()

	rang := rangeutils.Range{StartIndex: 0, EndIndex: uint64(hdfsExistFileContentLength)}
	body, responseHeader, err := sourceClient.DownloadWithResponseHeader(context.Background(), hdfsNotExistFileURL, source.RequestHeader{
		cdn.RangeHeaderName: fmt.Sprintf("bytes=%s", rang.String()),
	}, &rang)
	assert.EqualError(t, err, "open /user/root/input/f3.txt: file does not exist")
	assert.Nil(t, responseHeader)
	assert.Nil(t, body)
}

func TestGetLastModifiedMillis_FileExist(t *testing.T) {
	lastModified, _ := time.Parse(layout, hdfsExistFileLastModified)
	var info os.FileInfo = fakeHDFSFileInfo{
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
func (f fakeHDFSFileInfo) Mode() os.FileMode {
	if f.dir {
		return 0755 | os.ModeDir
	}
	return 0644
}
