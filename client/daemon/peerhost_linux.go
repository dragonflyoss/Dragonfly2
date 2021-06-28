// +build linux

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

package daemon

import (
	"fmt"
	"os"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"golang.org/x/sys/unix"
)

func switchNetNamespace(target string) (func() error, error) {
	fd, err := unix.Open(target, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	orgNS := fmt.Sprintf("/proc/%d/ns/net", os.Getpid())
	// hold the original net namespace
	orgFD, err := unix.Open(orgNS, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	logger.Infof("switch net namespace, from %s to %s", orgNS, target)

	err = unix.Setns(fd, unix.CLONE_NEWNET)
	if err != nil {
		return nil, err
	}

	return func() error {
		logger.Infof("recover net namespace, from %s to %s", target, orgNS)
		return unix.Setns(orgFD, unix.CLONE_NEWNET)
	}, nil
}
