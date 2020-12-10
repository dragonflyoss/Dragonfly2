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

package daemon

import (
	"fmt"
	"io"
	"math"
	"net"
	"net/http"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

type UploadManager interface {
	Serve(lis net.Listener) error
	Stop() error
}

type uploadManager struct {
	*http.Server
	StorageManager StorageManager
}

const (
	PeerHTTPPathPrefix = "/peer/file/"

	LocalHTTPPathCheck  = "/check/"
	LocalHTTPPathClient = "/client/"
	LocalHTTPPathRate   = "/rate/"
	LocalHTTPPing       = "/server/ping"
)

func NewUploadManager(s StorageManager) (UploadManager, error) {
	u := &uploadManager{
		StorageManager: s,
	}
	u.Server = &http.Server{
		Handler: u.initRouter(),
	}
	return u, nil
}

func (u *uploadManager) initRouter() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc(PeerHTTPPathPrefix+"{task:.*}", u.handleUpload).Methods("GET")
	//r.HandleFunc(LocalHTTPPathRate+"{task:.*}", u.handlerParseRate).Methods("GET")
	//r.HandleFunc(LocalHTTPPathCheck+"{task:.*}", u.handleCheck).Methods("GET")
	//r.HandleFunc(LocalHTTPPathClient+"finish", u.oneFinishHandler).Methods("GET")
	//r.HandleFunc(LocalHTTPPing, u.pingHandler).Methods("GET")
	return r
}

func (u *uploadManager) Serve(lis net.Listener) error {
	return u.Server.Serve(lis)
}

func (u *uploadManager) Stop() error {
	logrus.Warningf("TODO not implement")
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

	logrus.Debugf("upload task %s to %s, req: %#v", task, r.RemoteAddr, r.Header)
	reader, closer, err := u.StorageManager.GetPiece(r.Context(), &GetPieceRequest{
		TaskID: task,
		Range:  rg[0],
		Style:  0,
	})
	if err != nil {
		logrus.Errorf("get task %s data failed: %s", task, err)
		http.Error(w, fmt.Sprintf("get piece data error: %s", err), http.StatusInternalServerError)
		return
	}
	defer closer.Close()

	// if w is a socket, golang will use sendfile or splice syscall for zero copy feature
	if n, err := io.Copy(w, reader); err != nil {
		logrus.Errorf("tranfer task %s data failed: %s", task, err)
		http.Error(w, fmt.Sprintf("tranfer piece data error: %s", err), http.StatusInternalServerError)
		return
	} else if n != rg[0].Length {
		logrus.Errorf("tranfered task %s data length not match request, request: %d, transfered: %d",
			task, rg[0].Length, n)
		http.Error(w, fmt.Sprintf("piece data length not match request, request: %d, transfered: %d",
			rg[0].Length, n), http.StatusInternalServerError)
		return
	}
}

//func (u *uploadManager) handlerParseRate(w http.ResponseWriter, r *http.Request) {
//}

//// handleCheck is used to check the server status.
//func (u *uploadManager) handleCheck(w http.ResponseWriter, r *http.Request) {
//}

//func (u *uploadManager) pingHandler(w http.ResponseWriter, r *http.Request) {
//}

//func (u *uploadManager) oneFinishHandler(w http.ResponseWriter, r *http.Request) {
//}
