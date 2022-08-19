//go:build linux

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

	"golang.org/x/sys/unix"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

func switchNetNamespace(target string) (func() error, error) {
	fd, err := unix.Open(target, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

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
		if err := unix.Setns(orgFD, unix.CLONE_NEWNET); err != nil {
			logger.Errorf("recover net namespace, from %s to %s, error: %s", target, orgNS, err)
			return err
		}
		logger.Infof("recover net namespace, from %s to %s", target, orgNS)
		if err := unix.Close(orgFD); err != nil {
			logger.Errorf("recover net namespace, close fd error: %s", err)
			return err
		}
		logger.Infof("recover net namespace, close original fd")
		return nil
	}, nil
}
