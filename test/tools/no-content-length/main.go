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
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/textproto"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

var port = flag.Int("port", 80, "")

func main() {
	http.Handle("/", &fileHandler{dir: "/static"})
	log.Printf("listen on %d", *port)
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
	enableContentLength := f.enableContentLength
	if strings.ToLower(r.Header.Get("X-Dragonfly-Enable-Content-Length")) == "true" {
		enableContentLength = true
	}
	if r.Header.Get("X-Dragonfly-E2E-Status-Code") != "" {
		str := r.Header.Get("X-Dragonfly-E2E-Status-Code")
		code, err := strconv.Atoi(str)
		if err != nil {
			log.Printf("wrong X-Dragonfly-E2E-Status-Code format %s, error: %s", str, err)
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(fmt.Sprintf("wrong X-Dragonfly-E2E-Status-Code format")))
			return
		}
		w.WriteHeader(code)
		return
	}
	var rg *httpRange
	if s := r.Header.Get("Range"); s != "" {
		rgs, err := parseRange(s, math.MaxInt)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(fmt.Sprintf("wrong range format")))
			return
		}
		if len(rgs) > 1 || len(rgs) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(fmt.Sprintf("unsupport range format")))
			return
		}
		rg = &rgs[0]
	}
	upath := filepath.Clean(r.URL.Path)
	if !strings.HasPrefix(upath, "/") {
		upath = "/" + upath
		r.URL.Path = upath
	}

	filePath := path.Join(f.dir, upath)
	if !strings.HasPrefix(filePath, f.dir) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(fmt.Sprintf("target is not in correct dir")))
		return
	}
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
	var rd io.Reader
	if rg == nil {
		rd = file
	} else {
		_, _ = file.Seek(rg.start, io.SeekStart)
		rd = io.LimitReader(file, rg.length)
	}

	if enableContentLength {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))
	}
	_, _ = io.Copy(w, rd)
}

// httpRange specifies the byte range to be sent to the client.
type httpRange struct {
	start, length int64
}

// errNoOverlap is returned by serveContent's parseRange if first-byte-pos of
// all of the byte-range-spec values is greater than the content size.
var errNoOverlap = errors.New("invalid range: failed to overlap")

// parseRange parses a Range header string as per RFC 7233.
// errNoOverlap is returned if none of the ranges overlap.
func parseRange(s string, size int64) ([]httpRange, error) {
	if s == "" {
		return nil, nil // header not present
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, errors.New("invalid range")
	}
	var ranges []httpRange
	noOverlap := false
	for _, ra := range strings.Split(s[len(b):], ",") {
		ra = textproto.TrimString(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, errors.New("invalid range")
		}
		start, end := textproto.TrimString(ra[:i]), textproto.TrimString(ra[i+1:])
		var r httpRange
		if start == "" {
			// If no start is specified, end specifies the
			// range start relative to the end of the file,
			// and we are dealing with <suffix-length>
			// which has to be a non-negative integer as per
			// RFC 7233 Section 2.1 "Byte-Ranges".
			if end == "" || end[0] == '-' {
				return nil, errors.New("invalid range")
			}
			i, err := strconv.ParseInt(end, 10, 64)
			if i < 0 || err != nil {
				return nil, errors.New("invalid range")
			}
			if i > size {
				i = size
			}
			r.start = size - i
			r.length = size - r.start
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i < 0 {
				return nil, errors.New("invalid range")
			}
			if i >= size {
				// If the range begins after the size of the content,
				// then it does not overlap.
				noOverlap = true
				continue
			}
			r.start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.length = size - r.start
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.start > i {
					return nil, errors.New("invalid range")
				}
				if i >= size {
					i = size - 1
				}
				r.length = i - r.start + 1
			}
		}
		ranges = append(ranges, r)
	}
	if noOverlap && len(ranges) == 0 {
		// The specified ranges did not overlap with the content.
		return nil, errNoOverlap
	}
	return ranges, nil
}
