//go:build linux
// +build linux

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

package hdfsprotocol

import (
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/colinmarc/hdfs/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/pkg/net/http"
	"d7y.io/dragonfly/v2/pkg/source"
)

var sourceClient source.ResourceClient

const (
	hdfsExistFileHost                     = "127.0.0.1:9000"
	hdfsExistFilePath                     = "/user/root/input/f1.txt"
	hdfsExistFileURL                      = "hdfs://" + hdfsExistFileHost + hdfsExistFilePath
	hdfsExistFileContentLength      int64 = 11
	hdfsExistFileContent                  = "Hello World"
	hdfsExistFileLastModifiedMillis int64 = 1136214245000
	hdfsExistFileLastModified             = "Mon, 02 Jan 2006 15:04:05 GMT"
	hdfsExistFileRangeStart         int64 = 3
	hdfsExistFileRangeEnd           int64 = 10
)

const (
	hdfsNotExistFileURL                 = "hdfs://127.0.0.1:9000/user/root/input/f3.txt"
	hdfsNotExistFileContentLength int64 = source.UnknownSourceFileLen
	hdfsNotExistLastModified      int64 = -1
)

var fakeHDFSClient = &hdfs.Client{}

func testBefore() {
	sourceClient = newHDFSSourceClient(func(p *hdfsSourceClient) {
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
		{Values: gomonkey.Params{info, nil}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	contentLengthRequest, err := source.NewRequest(hdfsExistFileURL)
	assert.Nil(t, err)
	contentLengthRequest.Header.Add(source.Range, "0-12")
	// exist file
	length, err := sourceClient.GetContentLength(contentLengthRequest)
	assert.Equal(t, hdfsExistFileContentLength, length)
	assert.Nil(t, err)

}

// TestGetContentLength_Fail test file not exist, return error
func TestGetContentLength_Fail(t *testing.T) {

	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("stat /user/root/input/f3.txt: file does not exist")}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()
	hdfsNotExistFileRequest, err := source.NewRequest(hdfsExistFileURL)
	assert.Nil(t, err)
	hdfsNotExistFileRequest.Header.Add(source.Range, "0-10")
	// not exist file
	length, err := sourceClient.GetContentLength(hdfsNotExistFileRequest)
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
	request, err := source.NewRequest(hdfsExistFileURL)
	assert.Nil(t, err)
	supportRange, err := sourceClient.IsSupportRange(request)
	assert.Equal(t, true, supportRange)
	assert.Nil(t, err)
}

// TestIsSupportRange_FileNotExist test file not exist, return error and not support range
func TestIsSupportRange_FileNotExist(t *testing.T) {
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("stat /user/root/input/f3.txt: file does not exist")}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	request, err := source.NewRequest(hdfsNotExistFileURL)
	assert.Nil(t, err)
	supportRange, err := sourceClient.IsSupportRange(request)
	assert.Equal(t, false, supportRange)
	assert.EqualError(t, err, "stat /user/root/input/f3.txt: file does not exist")
}

func TestIsExpired_LastModifiedExpired(t *testing.T) {
	lastModified, _ := time.Parse(source.LastModifiedLayout, hdfsExistFileLastModified)
	var info os.FileInfo = fakeHDFSFileInfo{
		modtime: lastModified,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	request, err := source.NewRequest(hdfsExistFileURL)
	assert.Nil(t, err)
	// header have Last-Modified
	expired, err := sourceClient.IsExpired(request, &source.ExpireInfo{
		LastModified: time.Date(2020, 01, 01, 0, 0, 0, 0, time.UTC).Format(source.TimeFormat),
		ETag:         "",
	})
	assert.Equal(t, true, expired)
	assert.Nil(t, err)
}

func TestIsExpired_LastModifiedNotExpired(t *testing.T) {
	lastModified, _ := time.Parse(source.LastModifiedLayout, hdfsExistFileLastModified)
	var info os.FileInfo = fakeHDFSFileInfo{
		modtime: lastModified,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	request, err := source.NewRequest(hdfsExistFileURL)
	assert.Nil(t, err)
	// header have Last-Modified
	expired, err := sourceClient.IsExpired(request, &source.ExpireInfo{
		LastModified: hdfsExistFileLastModified,
		ETag:         "",
	})
	assert.Equal(t, false, expired)
	assert.Nil(t, err)
}

func Test_Download_FileExist_ByRange(t *testing.T) {
	var reader *hdfs.FileReader = &hdfs.FileReader{}
	patch := gomonkey.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return reader, nil
	})
	patch.ApplyMethod(reflect.TypeOf(reader), "Seek", func(_ *hdfs.FileReader, offset int64, whence int) (int64, error) {
		return 0 - hdfsExistFileContentLength, nil
	})
	patch.ApplyMethod(reflect.TypeOf(reader), "Read", func(_ *hdfs.FileReader, b []byte) (int, error) {
		bytes := []byte(hdfsExistFileContent)
		return copy(b, bytes), io.EOF
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

	rang := &http.Range{StartIndex: 0, EndIndex: uint64(hdfsExistFileContentLength) - 1}
	// exist file
	request, err := source.NewRequestWithHeader(hdfsExistFileURL, map[string]string{
		source.Range: rang.String(),
	})
	assert.Nil(t, err)

	response, err := sourceClient.Download(request)
	assert.Nil(t, err)
	data, _ := io.ReadAll(response.Body)

	assert.Equal(t, hdfsExistFileContent, string(data))
}

func TestDownload_FileNotExist(t *testing.T) {
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("open /user/root/input/f3.txt: file does not exist")}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Open", stubRet)

	defer patch.Reset()

	rang := http.Range{StartIndex: 0, EndIndex: uint64(hdfsExistFileContentLength)}

	request, err := source.NewRequestWithHeader(hdfsNotExistFileURL, map[string]string{
		source.Range: rang.String(),
	})
	assert.Nil(t, err)
	// not exist file
	download, err := sourceClient.Download(request)
	assert.Nil(t, download)
	assert.EqualError(t, err, "open /user/root/input/f3.txt: file does not exist")
}

func Test_DownloadWithResponseHeader_FileExist_ByRange(t *testing.T) {
	lastModified, _ := time.Parse(source.LastModifiedLayout, hdfsExistFileLastModified)
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

	rang := http.Range{StartIndex: uint64(hdfsExistFileRangeStart), EndIndex: uint64(hdfsExistFileRangeEnd)}
	request, err := source.NewRequest(hdfsExistFileURL)
	assert.Nil(t, err)
	request.Header.Add(source.Range, rang.String())
	response, err := sourceClient.Download(request)
	assert.Nil(t, err)
	assert.Equal(t, hdfsExistFileLastModified, response.ExpireInfo().LastModified)

	data, _ := io.ReadAll(response.Body)
	assert.Equal(t, string(data), string([]byte(hdfsExistFileContent)[hdfsExistFileRangeStart:hdfsExistFileRangeEnd]))
}

func TestDownloadWithResponseHeader_FileNotExist(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(fakeHDFSClient), "Open", func(*hdfs.Client, string) (*hdfs.FileReader, error) {
		return nil, errors.New("open /user/root/input/f3.txt: file does not exist")
	})
	defer patch.Reset()

	rang := http.Range{StartIndex: 0, EndIndex: uint64(hdfsExistFileContentLength)}
	request, err := source.NewRequest(hdfsNotExistFileURL)
	assert.Nil(t, err)
	request.Header.Add(source.Range, rang.String())
	response, err := sourceClient.Download(request)
	assert.EqualError(t, err, "open /user/root/input/f3.txt: file does not exist")
	assert.Nil(t, response)
}

func TestGetLastModified_FileExist(t *testing.T) {
	lastModified, _ := time.Parse(source.LastModifiedLayout, hdfsExistFileLastModified)
	var info os.FileInfo = fakeHDFSFileInfo{
		modtime: lastModified,
	}
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{info, nil}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	request, err := source.NewRequest(hdfsExistFileURL)
	assert.Nil(t, err)

	lastModifiedMillis, err := sourceClient.GetLastModified(request)
	assert.Nil(t, err)
	assert.Equal(t, hdfsExistFileLastModifiedMillis, lastModifiedMillis)
}

func TestGetLastModified_FileNotExist(t *testing.T) {
	stubRet := []gomonkey.OutputCell{
		{Values: gomonkey.Params{nil, errors.New("stat /user/root/input/f3.txt: file does not exist")}},
	}

	patch := gomonkey.ApplyMethodSeq(reflect.TypeOf(fakeHDFSClient), "Stat", stubRet)

	defer patch.Reset()

	request, err := source.NewRequest(hdfsNotExistFileURL)
	assert.Nil(t, err)

	lastModifiedMillis, err := sourceClient.GetLastModified(request)
	assert.EqualError(t, err, "stat /user/root/input/f3.txt: file does not exist")
	assert.Equal(t, hdfsNotExistLastModified, lastModifiedMillis)
}

func TestNewHDFSSourceClient(t *testing.T) {
	client := newHDFSSourceClient()
	assert.NotNil(t, client)

	options := make([]HDFSSourceClientOption, 0)

	option := func(p *hdfsSourceClient) {
		c, _ := hdfs.New(hdfsExistFileHost)
		p.clientMap[hdfsExistFileHost] = c
	}
	options = append(options, option)

	newHDFSSourceClient := newHDFSSourceClient(options...)

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
