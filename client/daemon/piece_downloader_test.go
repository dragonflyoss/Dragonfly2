package daemon

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/scheduler"
)

func TestPieceDownloader_DownloadPiece(t *testing.T) {
	assert := testifyassert.New(t)
	testData, err := ioutil.ReadFile(testFile)
	assert.Nil(err, "load test file")

	tests := []struct {
		handleFunc      func(w http.ResponseWriter, r *http.Request)
		taskID          string
		pieceRange      string
		targetPieceData []byte
	}{
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(r.URL.Path, "/peer/file/task-0")
				w.Write([]byte("test"))
			},
			taskID:          "task-0",
			pieceRange:      "bytes=0-9",
			targetPieceData: []byte("test"),
		},
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(r.URL.Path, "/peer/file/task-1")
				w.Write(testData[:100])
			},
			taskID:          "task-1",
			pieceRange:      "bytes=0-99",
			targetPieceData: testData[:100],
		},
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(r.URL.Path, "/peer/file/task-2")
				w.Write(testData[512:])
			},
			taskID:          "task-2",
			pieceRange:      fmt.Sprintf("bytes=512-%d", len(testData)-1),
			targetPieceData: testData[512:],
		},
		{
			handleFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(r.URL.Path, "/peer/file/task-3")
				w.Write(testData[512:1024])
			},
			taskID:          "task-3",
			pieceRange:      "bytes=512-1024",
			targetPieceData: testData[512:1024],
		},
	}

	for _, tt := range tests {
		server := httptest.NewServer(http.HandlerFunc(tt.handleFunc))
		addr, _ := url.Parse(server.URL)
		pd, _ := NewPieceDownloader()
		rc, err := pd.DownloadPiece(&DownloadPieceRequest{
			TaskID: tt.taskID,
			PiecePackage_PieceTask: &scheduler.PiecePackage_PieceTask{
				PieceRange: tt.pieceRange,
				PieceMd5:   "TODO",
				DstAddr:    addr.Host,
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
