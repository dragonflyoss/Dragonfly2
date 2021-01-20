package peer

import (
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/test"
	"github.com/dragonflyoss/Dragonfly2/client/daemon/upload"
	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/dfdaemon/server"
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
				w.Write([]byte("test test "))
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
				rg := util.MustParseRange(r.Header.Get("Range"), math.MaxInt64)
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
				rg := util.MustParseRange(r.Header.Get("Range"), math.MaxInt64)
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
				rg := util.MustParseRange(r.Header.Get("Range"), math.MaxInt64)
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
		pd, _ := NewPieceDownloader()
		rc, err := pd.DownloadPiece(&DownloadPieceRequest{
			TaskID:  tt.taskID,
			DstPid:  "",
			DstAddr: addr.Host,
			piece: &base.PieceInfo{
				PieceNum:    0,
				RangeStart:  tt.rangeStart,
				RangeSize:   tt.rangeSize,
				PieceMd5:    "TODO",
				PieceOffset: tt.rangeStart,
				PieceStyle:  base.PieceStyle_PLAIN,
			},
		})
		assert.Nil(err, "downloaded piece should success")

		data, err := ioutil.ReadAll(rc)
		assert.Nil(err, "read piece data should success")
		rc.Close()

		assert.Equal(data, tt.targetPieceData, "downloaded piece data should match")
		server.Close()
	}
}
