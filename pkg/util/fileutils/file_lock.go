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

package fileutils

import (
	"os"
	"syscall"

	"github.com/pkg/errors"
)

// FileLock defines a file lock implemented by syscall.Flock.
// Locks created by flock() are associated with an open file
// description.  This means that duplicate file
// descriptors (created by, for example, fork or dup) refer to
// the same lock, and this lock may be modified or released using
// any of these file descriptors. Furthermore, the lock is released
// either by an explicit LOCK_UN operation on any of these duplicate
// file descriptors, or when all such file descriptors have been closed.
type FileLock struct {
	fileName string
	ofile    *os.File
}

func NewFileLock(path string) (*FileLock, error) {
	ofile, err := OpenFile(path, syscall.O_CREAT|syscall.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &FileLock{
		fileName: path,
		ofile:    ofile,
	}, nil
}

func (locker *FileLock) Lock() error {
	if locker.ofile == nil {
		return errors.Errorf("lock file %s: not open", locker.fileName)
	}

	if err := syscall.Flock(int(locker.ofile.Fd()), syscall.LOCK_EX); err != nil {
		return errors.Wrapf(err, "failed to lock file %s", locker.fileName)
	}

	return nil
}

func (locker *FileLock) TryLock() error {
	if locker.ofile == nil {
		return errors.Errorf("try lock file %s: not open", locker.fileName)
	}

	return syscall.Flock(int(locker.ofile.Fd()), syscall.LOCK_NB|syscall.LOCK_EX)
}

func (locker *FileLock) Unlock() error {
	if locker.ofile == nil {
		return nil
	}

	if err := syscall.Flock(int(locker.ofile.Fd()), syscall.LOCK_UN); err != nil {
		return errors.Wrapf(err, "failed to unlock file %s", locker.fileName)
	}

	return nil
}
