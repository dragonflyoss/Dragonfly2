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

package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
)

var port = flag.Int("port", 80, "")

func main() {
	http.Handle("/", &fileHandler{dir: "static"})
	fmt.Printf("listen on %d", *port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	if err != nil {
		panic(err)
	}
}

type fileHandler struct {
	dir                 string
	enableContentLength bool
}

func (f *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upath := r.URL.Path
	if !strings.HasPrefix(upath, "/") {
		upath = "/" + upath
		r.URL.Path = upath
	}

	filePath := path.Join(f.dir, r.URL.Path)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, _ = w.Write([]byte(fmt.Sprintf("%s", err)))
		return
	}
	if fileInfo.IsDir() {
		// todo list files
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(fmt.Sprintf("target is dir not file")))
		return
	}
	file, err := os.Open(filePath)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("%s", err)))
		return
	}
	defer file.Close()

	if f.enableContentLength {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))
	}
	_, _ = io.Copy(w, file)
}
