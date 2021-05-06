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

// Package pidfile provides structure and helper functions to create and remove
// PID file. A PID file is usually a file used to store the process ID of a
// running process.
package pidfile // copy from "github.com/docker/docker/pkg/pidfile"

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/process"
)

// PIDFile is a file used to store the process ID of a running process.
type PIDFile struct {
	path string
}

func IsProcessExistsByPIDFile(path string) (bool, error) {
	pidByte, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	pid, err := strconv.ParseInt(strings.TrimSpace(string(pidByte)), 10, 32)
	if err != nil {
		return false, err
	}
	return process.PidExists(int32(pid))
}

// New creates a PIDfile using the specified path.
func New(path string) (*PIDFile, error) {
	if ok, err := IsProcessExistsByPIDFile(path); ok {
		return nil, fmt.Errorf("process already exists")
	} else if err != nil {
		return nil, err
	}
	// Note MkdirAll returns nil if a directory already exists
	if err := os.MkdirAll(filepath.Dir(path), os.FileMode(0755)); err != nil {
		return nil, err
	}
	if err := ioutil.WriteFile(path, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
		return nil, err
	}

	return &PIDFile{path: path}, nil
}

// Remove removes the PIDFile.
func (file PIDFile) Remove() error {
	return os.Remove(file.path)
}
