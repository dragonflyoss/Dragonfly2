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

package util

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const (
	defaultFileMode = os.FileMode(0664)
)

func UploadArtifactStdout(namespace, podName, logDirName, logPrefix string) error {
	out, err := KubeCtlCommand("-n", namespace, "logs", podName).CombinedOutput()
	if err != nil {
		return err
	}

	logFileName := fmt.Sprintf("%s-stdout.log", logPrefix)
	logDirname := fmt.Sprintf("/tmp/artifact/%s/", logDirName)
	if err := os.WriteFile(logFileName, out, defaultFileMode); err != nil {
		return err
	}

	if out, err := exec.Command("mv", logFileName, filepath.Join(logDirname, logFileName)).CombinedOutput(); err != nil {
		fmt.Printf("move %s error: %s\n", logFileName, out)
		return err
	}

	return nil
}

func UploadArtifactPrevStdout(namespace, podName, logDirName, logPrefix string) error {
	out, err := KubeCtlCommand("-n", namespace, "logs", podName, "-p").CombinedOutput()
	if err != nil {
		return err
	}

	logFileName := fmt.Sprintf("%s-prev-stdout.log", logPrefix)
	logDirname := fmt.Sprintf("/tmp/artifact/%s/", logDirName)
	if err := os.WriteFile(logFileName, out, defaultFileMode); err != nil {
		return err
	}

	if out, err := exec.Command("mv", logFileName, filepath.Join(logDirname, logFileName)).CombinedOutput(); err != nil {
		fmt.Printf("move %s error: %s\n", logFileName, out)
		return err
	}

	return nil
}

func UploadArtifactPProf(namespace, podName, logDirName, logPrefix string, pprofPort int) (string, error) {
	logDirname := fmt.Sprintf("/tmp/artifact/%s/", logDirName)
	out, err := KubeCtlCommand("-n", namespace, "exec", podName, "--", "sh", "-c", fmt.Sprintf(`
      set -x
      port=%d
      dir=%s
      prefix=%s
      ip=$(hostname -i)
      wget $ip:$port/debug/pprof/"goroutine?debug=1" -O $dir/$prefix-pprof-goroutine-1.log
      wget $ip:$port/debug/pprof/"goroutine?debug=2" -O $dir/$prefix-pprof-goroutine-2.log
      `, pprofPort, logDirname, logPrefix)).CombinedOutput()
	return string(out), err
}
