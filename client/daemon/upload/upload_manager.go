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

package upload

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"

	"github.com/go-http-utils/headers"
	"github.com/gorilla/mux"
	"golang.org/x/time/rate"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/storage"
	logger "d7y.io/dragonfly/v2/internal/dflog"
)

var _ *logger.SugaredLoggerOnWith // pin this package for no log code generation

type Manager interface {
	Serve(lis net.Listener) error
	Stop() error
}

type uploadManager struct {
	*http.Server
	*rate.Limiter
	StorageManager storage.Manager
}

var _ Manager = (*uploadManager)(nil)

const (
	PeerDownloadHTTPPathPrefix = "/download/"
)

func NewUploadManager(s storage.Manager, opts ...func(*uploadManager)) (Manager, error) {
	u := &uploadManager{
		Server:         &http.Server{},
		StorageManager: s,
	}
	u.initRouter()
	for _, opt := range opts {
		opt(u)
	}
	return u, nil
}

// WithLimiter sets upload rate limiter, the burst size must big than piece size
func WithLimiter(limiter *rate.Limiter) func(*uploadManager) {
	return func(manager *uploadManager) {
		manager.Limiter = limiter
	}
}

func (um *uploadManager) initRouter() {
	r := mux.NewRouter()
	r.HandleFunc(PeerDownloadHTTPPathPrefix+"{taskPrefix:.*}/"+"{task:.*}", um.handleUpload).Queries("peerId", "{.*}").Methods("GET")
	um.Server.Handler = r
}

func (um *uploadManager) Serve(lis net.Listener) error {
	return um.Server.Serve(lis)
}

func (um *uploadManager) Stop() error {
	return um.Server.Shutdown(context.Background())
}

// handleUpload uses to upload a task file when other peers download from it.
func (um *uploadManager) handleUpload(w http.ResponseWriter, r *http.Request) {
	var (
		task = mux.Vars(r)["task"]
		peer = r.FormValue("peerId")
		//cdnSource = r.Header.Get("X-Dragonfly-CDN-Source")
	)

	sLogger := logger.With("peer", peer, "task", task, "component", "uploadManager")
	sLogger.Debugf("upload piece for task %s/%s to %s, request header: %#v", task, peer, r.RemoteAddr, r.Header)
	rg, err := clientutil.ParseRange(r.Header.Get(headers.Range), math.MaxInt64)
	if err != nil {
		sLogger.Errorf("parse range with error: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(rg) != 1 {
		sLogger.Error("multi range parsed, not support")
		http.Error(w, "invalid range", http.StatusBadRequest)
		return
	}

	// add header "Content-Length" to avoid chunked body in http client
	w.Header().Add(headers.ContentLength, fmt.Sprintf("%d", rg[0].Length))
	reader, closer, err := um.StorageManager.ReadPiece(r.Context(),
		&storage.ReadPieceRequest{
			PeerTaskMetaData: storage.PeerTaskMetaData{
				TaskID: task,
				PeerID: peer,
			},
			PieceMetaData: storage.PieceMetaData{
				Num:   -1,
				Range: rg[0],
			},
		})
	if err != nil {
		sLogger.Errorf("get task data failed: %s", err)
		http.Error(w, fmt.Sprintf("get piece data error: %s", err), http.StatusInternalServerError)
		return
	}
	defer closer.Close()
	if um.Limiter != nil {
		if err = um.Limiter.WaitN(r.Context(), int(rg[0].Length)); err != nil {
			sLogger.Errorf("get limit failed: %s", err)
			http.Error(w, fmt.Sprintf("get limit error: %s", err), http.StatusInternalServerError)
			return
		}
	}

	// if w is a socket, golang will use sendfile or splice syscall for zero copy feature
	// when start to transfer data, we could not call http.Error with header
	if n, err := io.Copy(w, reader); err != nil {
		sLogger.Errorf("transfer data failed: %s", err)
		return
	} else if n != rg[0].Length {
		sLogger.Errorf("transferred data length not match request, request: %d, transferred: %d",
			rg[0].Length, n)
		return
	}
}
