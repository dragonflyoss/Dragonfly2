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

package cmd

import (
	"os"
	"reflect"
	"strconv"
	"testing"

	"d7y.io/dragonfly/v2/client/config"
)

func TestInitDaemonArgs(t *testing.T) {
	dfgetConfig := config.NewDfgetConfig()
	pid := strconv.Itoa(os.Getpid())

	baseArgs := []string{"daemon", "--launcher", pid}

	// Test daemon args without workhome and logdir
	daemonArgs := initDaemonArgs(dfgetConfig)

	if reflect.DeepEqual(daemonArgs, baseArgs) == false {
		t.Errorf("Expected %v, got %v", baseArgs, daemonArgs)
	}

	// Test daemon args with workhome and without logdir
	dfgetConfig.WorkHome = "/workhome1"
	daemonArgs = initDaemonArgs(dfgetConfig)
	if reflect.DeepEqual(daemonArgs, append(baseArgs, "--workhome", "/workhome1")) == false {
		t.Errorf("Expected %v, got %v", append(baseArgs, "--workhome", "/workhome1"), daemonArgs)
	}
	// unset workhome
	dfgetConfig.WorkHome = ""

	// Test daemon args with logdir and without workhome
	dfgetConfig.LogDir = "/logdir1"
	daemonArgs = initDaemonArgs(dfgetConfig)
	if reflect.DeepEqual(daemonArgs, append(baseArgs, "--logdir", "/logdir1")) == false {
		t.Errorf("Expected %v, got %v", append(baseArgs, "--logdir", "/logdir1"), daemonArgs)
	}

	// unset logdir
	dfgetConfig.LogDir = ""

	// Test daemon args with workhome and logdir
	dfgetConfig.WorkHome = "/workhome2"
	dfgetConfig.LogDir = "/logdir2"

	daemonArgs = initDaemonArgs(dfgetConfig)
	if reflect.DeepEqual(daemonArgs, append(baseArgs, "--workhome", "/workhome2", "--logdir", "/logdir2")) == false {
		t.Errorf("Expected %v, got %v", append(baseArgs, "--workhome", "/tmp", "--logdir", "/tmp"), daemonArgs)
	}
}
