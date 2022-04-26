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

package fileserver

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
)

// d7yDir is modified from http.Dir, for http.Dir does not treat syscall.EMFILE as we want.
type d7yDir string

func mapDirOpenError(originalErr error, name string) error {
	if os.IsNotExist(originalErr) || os.IsPermission(originalErr) {
		return originalErr
	}

	parts := strings.Split(name, string(filepath.Separator))
	for i := range parts {
		if parts[i] == "" {
			continue
		}
		fi, err := os.Stat(strings.Join(parts[:i+1], string(filepath.Separator)))
		if err != nil {
			return originalErr
		}
		if !fi.IsDir() {
			if pathError, ok := originalErr.(*fs.PathError); ok {
				return pathError.Err
			}
			return fs.ErrNotExist
		}
	}
	return originalErr
}

func (d d7yDir) Open(name string) (http.File, error) {
	if filepath.Separator != '/' && strings.ContainsRune(name, filepath.Separator) {
		return nil, errors.New("http: invalid character in file path")
	}
	dir := string(d)
	if dir == "" {
		dir = "."
	}
	fullName := filepath.Join(dir, filepath.FromSlash(path.Clean("/"+name)))

	f, err := os.Open(fullName)
	if err != nil {
		mappedErr := mapDirOpenError(err, fullName)
		// retry until success when syscall.EMFILE
		for mappedErr == syscall.EMFILE {
			time.Sleep(100 * time.Millisecond)
			f, err = os.Open(fullName)
			if err == nil {
				return f, nil
			}
			mappedErr = mapDirOpenError(err, fullName)
		}
		return nil, mappedErr
	}
	return f, nil
}

type Server struct {
	*http.Server
}

func New(port int, prefix, uploadPath string) *Server {
	return &Server{
		&http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: http.StripPrefix(prefix, http.FileServer(d7yDir(uploadPath))),
		},
	}
}
