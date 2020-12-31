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
	"fmt"
	"io"
	"math"
	"net"
	"net/http"

	"github.com/gorilla/mux"
	"golang.org/x/time/rate"

	"github.com/dragonflyoss/Dragonfly2/client/daemon/storage"
	"github.com/dragonflyoss/Dragonfly2/client/util"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
)

type Manager interface {
	Serve(lis net.Listener) error
	Stop() error
}

type uploadManager struct {
	*http.Server
	*rate.Limiter
	StorageManager storage.Manager
}

const (
	PeerDownloadHTTPPathPrefix = "/download/"
)

func NewUploadManager(s storage.Manager, opts ...func(*uploadManager)) (Manager, error) {
	u := &uploadManager{
		StorageManager: s,
		Server:         &http.Server{},
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

func (u *uploadManager) initRouter() {
	r := mux.NewRouter()
	r.HandleFunc(PeerDownloadHTTPPathPrefix+"{taskPrefix:.*}/"+"{task:.*}", u.handleUpload).Methods("GET")
	u.Server.Handler = r
}

func (u *uploadManager) Serve(lis net.Listener) error {
	return u.Server.Serve(lis)
}

func (u *uploadManager) Stop() error {
	logger.Warnf("TODO not implement")
	return nil
}

// handleUpload uses to upload a task file when other peers download from it.
func (u *uploadManager) handleUpload(w http.ResponseWriter, r *http.Request) {
	var (
		task = mux.Vars(r)["task"]
		//cdnSource = r.Header.Get("X-Dragonfly-CDN-Source")
	)

	rg, err := util.ParseRange(r.Header.Get("Range"), math.MaxInt64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(rg) != 1 {
		http.Error(w, "invalid range", http.StatusBadRequest)
		return
	}
	//w.Header().Add("X-Dragonfly-CDN-Source", "")

	logger.Debugf("upload piece for task %s to %s, request header: %#v", task, r.RemoteAddr, r.Header)
	reader, closer, err := u.StorageManager.ReadPiece(r.Context(),
		&storage.ReadPieceRequest{
			PeerTaskMetaData: storage.PeerTaskMetaData{
				TaskID: task,
			},
			PieceMetaData: storage.PieceMetaData{
				Range: rg[0],
				Style: 0,
			},
		})
	if err != nil {
		logger.Errorf("get task %s data failed: %s", task, err)
		http.Error(w, fmt.Sprintf("get piece data error: %s", err), http.StatusInternalServerError)
		return
	}
	defer closer.Close()
	if u.Limiter != nil {
		if err = u.Limiter.WaitN(r.Context(), int(rg[0].Length)); err != nil {
			logger.Errorf("get limit for %s failed: %s", task, err)
			http.Error(w, fmt.Sprintf("get limit error: %s", err), http.StatusInternalServerError)
			return
		}
	}

	// if w is a socket, golang will use sendfile or splice syscall for zero copy feature
	if n, err := io.Copy(w, reader); err != nil {
		logger.Errorf("transfer task %s data failed: %s", task, err)
		http.Error(w, fmt.Sprintf("tranfer piece data error: %s", err), http.StatusInternalServerError)
		return
	} else if n != rg[0].Length {
		logger.Errorf("transferred task %s data length not match request, request: %d, transferred: %d",
			task, rg[0].Length, n)
		http.Error(w, fmt.Sprintf("piece data length not match request, request: %d, transfered: %d",
			rg[0].Length, n), http.StatusInternalServerError)
		return
	}
}
