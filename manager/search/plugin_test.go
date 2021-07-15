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

package search

import (
	"os"
	"os/exec"
	"testing"

	"d7y.io/dragonfly/v2/internal/dfpath"
	"d7y.io/dragonfly/v2/manager/model"
	testifyassert "github.com/stretchr/testify/assert"
)

func Test_loadPlugin(t *testing.T) {
	assert := testifyassert.New(t)
	defer func() {
		os.Remove("./testdata/d7y-algorithm-plugin-search.so")
	}()

	// build plugin
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o=./testdata/d7y-algorithm-plugin-search.so", "testdata/plugin/search.go")
	output, err := cmd.CombinedOutput()
	assert.Nil(err)
	if err != nil {
		t.Fatalf(string(output))
		return
	}

	dfpath.PluginsDir = "."

	s, err := loadPlugin()
	assert.Nil(err)
	if err != nil {
		t.Fatal(err)
		return
	}

	cluster, ok := s.SchedulerCluster([]model.SchedulerCluster{}, map[string]string{})
	assert.Equal(ok, true)
	assert.Equal(cluster.Name, "foo")
}
