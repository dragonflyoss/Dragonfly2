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

package pidfile

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/process"
)

// PIDFile is a file used to store the process ID and cmdline of a running process.
type PIDFile struct {
	path    string
	pid     int
	cmdline string
}

func IsProcessExistsByPIDFile(path string) (bool, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return false, err
	}

	content := strings.TrimSpace(string(bytes))
	index := strings.LastIndex(content, "@")
	if index == -1 {
		return false, errors.New("pid file content is invalid")
	}

	pid, err := strconv.Atoi(content[index+1:])
	if err != nil {
		return false, err
	}
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return false, err
	}
	cmdline, _ := p.Cmdline()
	return strings.TrimSpace(cmdline+"@"+strconv.Itoa(pid)) == content, nil
}

// New creates a PIDFile using the specified path.
func New(path string) (*PIDFile, error) {
	//if ok, _ := IsProcessExistsByPIDFile(path); ok {
	//	return nil, errors.Errorf("process already exists")
	//}

	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}

	cmdline, _ := p.Cmdline()

	if err := ioutil.WriteFile(path, []byte(fmt.Sprintf("%s@%d", cmdline, p.Pid)), 0644); err != nil {
		return nil, err
	}

	return &PIDFile{path: path, pid: int(p.Pid), cmdline: cmdline}, nil
}

// Remove removes the PIDFile.
func (pf *PIDFile) Remove() error {
	return fileutils.DeleteFile(pf.path)
}
