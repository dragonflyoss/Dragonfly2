/*
 *     Copyright 2022 The Dragonfly Authors
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

package user

import (
	"fmt"
	"os"
	"os/user"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// HomeDir is the path to the user's home directory.
func HomeDir() string {
	u, err := user.Current()
	if err != nil {
		logger.Warnf("Failed to get current user: %s. Use / as HomeDir", err.Error())
		return "/"
	}

	return u.HomeDir
}

// Username is the username.
func Username() string {
	u, err := user.Current()
	if err != nil {
		logger.Warnf("Failed to get current user: %s. Use os.Getuid() as username", err.Error())
		return fmt.Sprint(os.Getuid())
	}

	return u.Username
}
