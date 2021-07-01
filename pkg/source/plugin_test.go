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

package source

import (
	"os"
	"os/exec"
	"path"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
)

func Test_loadPlugin(t *testing.T) {
	assert := testifyassert.New(t)
	defer func() {
		os.Remove("./testdata/d7y-resource-plugin-dfs.so")
		os.Remove("./testdata/test")
	}()

	var (
		cmd    *exec.Cmd
		output []byte
		wd     string
		err    error
	)

	// TODO can not load golang plugin in testing, because the different building flags
	// golang runtime will check the runtime hash of all imported packages

	// build plugin
	cmd = exec.Command("go", "build", "-buildmode=plugin", "-o=./testdata/d7y-resource-plugin-dfs.so", "testdata/plugin/dfs.go")
	output, err = cmd.CombinedOutput()
	assert.Nil(err)
	if err != nil {
		t.Fatalf(string(output))
		return
	}

	// build test binary
	cmd = exec.Command("go", "build", "-o=./testdata/test", "testdata/main.go")
	output, err = cmd.CombinedOutput()
	assert.Nil(err)
	if err != nil {
		t.Fatalf(string(output))
		return
	}

	wd, err = os.Getwd()
	assert.Nil(err)
	wd = path.Join(wd, "testdata")

	// execute test binary
	cmd = exec.Command("./testdata/test", "-plugin-dir", wd)
	output, err = cmd.CombinedOutput()
	assert.Nil(err)
	if err != nil {
		t.Fatalf(string(output))
		return
	}
}
