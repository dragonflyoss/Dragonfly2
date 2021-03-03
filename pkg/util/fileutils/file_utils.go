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

// Package fileutils provides utilities supplementing the standard about file packages.
package fileutils

import (
	"io"
	"os"
	"path/filepath"
	"syscall"

	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/util/fileutils/fsize"
	"github.com/pkg/errors"
)

// MkdirAll creates a directory named path with 0755 perm.
func MkdirAll(path string) error {
	return os.MkdirAll(path, 0755)
}

// DeleteFile deletes a regular file not a directory.
func DeleteFile(path string) error {
	if PathExist(path) {
		if IsDir(path) {
			return errors.Errorf("delete file %s: not a regular file", path)
		}

		return os.Remove(path)
	}

	return nil
}

// OpenFile opens a file. If the parent directory of the file isn't exist,
// it will create the directory.
func OpenFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	if PathExist(path) {
		return os.OpenFile(path, flag, perm)
	}

	if (flag & syscall.O_CREAT) > 0 {
		if err := MkdirAll(filepath.Dir(path)); err != nil {
			return nil, errors.Wrapf(err, "failed to open file %s", path)
		}
	}

	return os.OpenFile(path, flag, perm)
}

func Open(path string) (*os.File, error) {
	return OpenFile(path, syscall.O_RDONLY, 0)
}

// Link creates a hard link pointing to oldname named newname for a file.
func Link(oldname string, newname string) error {
	if PathExist(newname) {
		if IsDir(newname) {
			return errors.Errorf("link %s to %s: src already exists and is a directory", newname, oldname)
		}

		if err := DeleteFile(newname); err != nil {
			return errors.Wrapf(err, "failed to link %s to %s", newname, oldname)
		}
	} else if err := MkdirAll(filepath.Dir(newname)); err != nil {
		return errors.Wrapf(err, "failed to link %s to %s", newname, oldname)
	}

	return os.Link(oldname, newname)
}

// SymbolicLink creates newname as a symbolic link to oldname.
func SymbolicLink(oldname string, newname string) error {
	if !PathExist(oldname) {
		return errors.Errorf("symlink %s to %s: no such dst file", newname, oldname)
	}

	if IsDir(oldname) {
		return errors.Errorf("symlink %s to %s: dst is a directory", newname, oldname)
	}

	if PathExist(newname) {
		if IsDir(newname) {
			return errors.Errorf("symlink %s to %s: src already exists and is a directory", newname, oldname)
		}

		if err := DeleteFile(newname); err != nil {
			return errors.Wrapf(err, "failed to symlink %s to %s", newname, oldname)
		}
	} else if err := MkdirAll(filepath.Dir(newname)); err != nil {
		return errors.Wrapf(err, "failed to symlink %s to %s", newname, oldname)
	}

	return os.Symlink(oldname, newname)
}

// PathExist reports whether the path is exist.
// Any error, from stat(), will return false.
func PathExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func IsDir(path string) bool {
	f, err := stat(path)
	if err != nil {
		return false
	}

	return f.IsDir()
}

func IsRegular(path string) bool {
	f, err := stat(path)
	if err != nil {
		return false
	}

	return f.Mode().IsRegular()
}

// GetSysStat returns underlying data source of the os.FileInfo.
func GetSysStat(info os.FileInfo) *syscall.Stat_t {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return stat
	} else {
		return nil
	}
}

func FreeSpace(diskPath string) (fsize.Size, error) {
	fs := &syscall.Statfs_t{}
	if err := syscall.Statfs(diskPath, fs); err != nil {
		return fsize.ToFsize(0), err
	}

	return fsize.ToFsize(int64(fs.Bavail) * int64(fs.Bsize)), nil
}

func IsEmptyDir(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	if _, err = f.Readdirnames(1); err == io.EOF {
		return true, nil
	}

	return false, err
}

func stat(path string) (os.FileInfo, error) {
	f, err := os.Stat(path)

	if err != nil {
		logger.Warnf("stat file %s: %v", path, err)
	}

	return f, err
}
