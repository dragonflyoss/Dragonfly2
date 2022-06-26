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

package basic

import (
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

var (
	HomeDir   string
	TmpDir    string
	Username  string
	UserID    int
	UserGroup int
)

func init() {
	u, err := user.Current()
	if err != nil {
		logger.Warnf("Failed to get current user: %s", err.Error())
		logger.Info("Use uid as Username")

		UserID = syscall.Getuid()
		Username = fmt.Sprintf("%d", UserID)
		UserGroup = syscall.Getgid()
		HomeDir = "/"
	} else {
		Username = u.Username
		UserID, _ = strconv.Atoi(u.Uid)
		UserGroup, _ = strconv.Atoi(u.Gid)

		HomeDir = u.HomeDir
		HomeDir = strings.TrimSpace(HomeDir)
		if pkgstrings.IsBlank(HomeDir) {
			panic("home dir is empty")
		}
	}

	TmpDir = os.TempDir()
	TmpDir = strings.TrimSpace(TmpDir)
	if pkgstrings.IsBlank(TmpDir) {
		TmpDir = "/tmp"
	}
}
