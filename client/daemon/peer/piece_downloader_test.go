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

package peer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-http-utils/headers"
	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/test"
	"d7y.io/dragonfly/v2/client/daemon/upload"
	"d7y.io/dragonfly/v2/internal/rpc/base"
	_ "d7y.io/dragonfly/v2/internal/rpc/dfdaemon/server"
)

func TestPieceDownloader_DownloadPiece(t *testing.T) {
	assert := testifyassert.New(t)
	testData, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	tests := []struct {
		handleFunc      func(w http.ResponseWriter, r *http.Request)
		taskID          string
		pieceRange      string
		rangeStart      uint64
		rangeSize       int32
		targetPieceData []byte
	}{
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(upload.PeerDownloadHTTPPathPrefix+"tas/"+"task-0", r.URL.Path)
				data := []byte("test test ")
				w.Header().Set(headers.ContentLength, fmt.Sprintf("%d", len(data)))
				w.Write(data)
			},
			taskID:          "task-0",
			pieceRange:      "bytes=0-9",
			rangeStart:      0,
			rangeSize:       10,
			targetPieceData: []byte("test test "),
		},
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(upload.PeerDownloadHTTPPathPrefix+"tas/"+"task-1", r.URL.Path)
				rg := clientutil.MustParseRange(r.Header.Get("Range"), math.MaxInt64)
				w.Header().Set(headers.ContentLength, fmt.Sprintf("%d", rg.Length))
				w.Write(testData[rg.Start : rg.Start+rg.Length])
			},
			taskID:          "task-1",
			pieceRange:      "bytes=0-99",
			rangeStart:      0,
			rangeSize:       100,
			targetPieceData: testData[:100],
		},
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(upload.PeerDownloadHTTPPathPrefix+"tas/"+"task-2", r.URL.Path)
				rg := clientutil.MustParseRange(r.Header.Get("Range"), math.MaxInt64)
				w.Header().Set(headers.ContentLength, fmt.Sprintf("%d", rg.Length))
				w.Write(testData[rg.Start : rg.Start+rg.Length])
			},
			taskID:          "task-2",
			pieceRange:      fmt.Sprintf("bytes=512-%d", len(testData)-1),
			rangeStart:      512,
			rangeSize:       int32(len(testData) - 512),
			targetPieceData: testData[512:],
		},
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(upload.PeerDownloadHTTPPathPrefix+"tas/"+"task-3", r.URL.Path)
				rg := clientutil.MustParseRange(r.Header.Get("Range"), math.MaxInt64)
				w.Header().Set(headers.ContentLength, fmt.Sprintf("%d", rg.Length))
				w.Write(testData[rg.Start : rg.Start+rg.Length])
			},
			taskID:          "task-3",
			pieceRange:      "bytes=512-1024",
			rangeStart:      512,
			rangeSize:       513,
			targetPieceData: testData[512:1025],
		},
	}

	for _, tt := range tests {
		server := httptest.NewServer(http.HandlerFunc(tt.handleFunc))
		addr, _ := url.Parse(server.URL)
		factories := []func() (PieceDownloader, error){
			func() (PieceDownloader, error) {
				return NewPieceDownloader()
			}, func() (PieceDownloader, error) {
				return NewOptimizedPieceDownloader()
			}}
		for _, factory := range factories {
			pd, _ := factory()
			hash := md5.New()
			hash.Write(tt.targetPieceData)
			digest := hex.EncodeToString(hash.Sum(nil)[:16])
			r, c, err := pd.DownloadPiece(context.Background(), &DownloadPieceRequest{
				TaskID:     tt.taskID,
				DstPid:     "",
				DstAddr:    addr.Host,
				CalcDigest: true,
				piece: &base.PieceInfo{
					PieceNum:    0,
					RangeStart:  tt.rangeStart,
					RangeSize:   tt.rangeSize,
					PieceMd5:    digest,
					PieceOffset: tt.rangeStart,
					PieceStyle:  base.PieceStyle_PLAIN,
				},
			})
			assert.Nil(err, "downloaded piece should success")

			data, err := ioutil.ReadAll(r)
			assert.Nil(err, "read piece data should success")
			c.Close()

			assert.Equal(data, tt.targetPieceData, "downloaded piece data should match")
		}
		server.Close()
	}
}
