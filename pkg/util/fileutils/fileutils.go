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

// Package fileutils provides utilities supplementing the standard 'os' and 'path' package.
package fileutils

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

// MkdirAll creates a directory named path on perm(0755).
func MkdirAll(path string) error {
	return os.MkdirAll(path, 0755)
}

// DeleteFile deletes a regular file not a directory.
func DeleteFile(path string) error {
	if PathExist(path) {
		if IsDir(path) {
			return errors.Errorf("delete %s: not a regular file", path)
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

	if err := MkdirAll(filepath.Dir(path)); err != nil {
		return nil, err
	}

	return os.OpenFile(path, flag, perm)
}

// Link creates a hard link pointing to oldname named newname for a file.
func Link(oldname string, newname string) error {
	if PathExist(newname) {
		if IsDir(newname) {
			return errors.Errorf("link %s to %s: link name already exists and is a directory", newname, oldname)
		}

		if err := DeleteFile(newname); err != nil {
			return errors.Errorf("link %s to %s: link name already exists and deleting fail: %v", newname, oldname, err)
		}
	}

	return os.Link(oldname, newname)
}

// SymbolicLink creates newname as a symbolic link to oldname.
func SymbolicLink(oldname string, newname string) error {
	if !PathExist(oldname) {
		return errors.Errorf("symlink %s to %s: src no such file or directory", newname, oldname)
	}

	if PathExist(newname) {
		if IsDir(newname) {
			return fmt.Errorf("failed to symlink %s to %s: link name already exists and is a directory", newname, oldname)
		}
		if err := DeleteFile(newname); err != nil {
			return fmt.Errorf("failed to symlink %s to %s when deleting target file: %v", newname, oldname, err)
		}
	}

	return os.Symlink(oldname, newname)
}

// CopyFile copies the file src to dst.
func CopyFile(dst string, src string) (written int64, err error) {
	var (
		s *os.File
		d *os.File
	)
	if !IsRegularFile(src) {
		return 0, fmt.Errorf("failed to copy %s to %s: src is not a regular file", src, dst)
	}
	if s, err = os.Open(src); err != nil {
		return 0, fmt.Errorf("failed to copy %s to %s when opening source file: %v", src, dst, err)
	}
	defer s.Close()

	if PathExist(dst) {
		return 0, fmt.Errorf("failed to copy %s to %s: dst file already exists", src, dst)
	}

	if d, err = OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return 0, fmt.Errorf("failed to copy %s to %s when opening destination file: %v", src, dst, err)
	}
	defer d.Close()

	return io.Copy(d, s)
}

// MoveFile moves the file src to dst.
func MoveFile(src string, dst string) error {
	if !IsRegularFile(src) {
		return fmt.Errorf("failed to move %s to %s: src is not a regular file", src, dst)
	}
	if PathExist(dst) && !IsDir(dst) {
		if err := DeleteFile(dst); err != nil {
			return fmt.Errorf("failed to move %s to %s when deleting dst file: %v", src, dst, err)
		}
	}
	return os.Rename(src, dst)
}

// PathExist reports whether the path is exist.
// Any error get from os.Stat, it will return false.
func PathExist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// IsDir reports whether the path is a directory.
func IsDir(name string) bool {
	f, e := os.Stat(name)
	if e != nil {
		return false
	}
	return f.IsDir()
}

// IsRegularFile reports whether the file is a regular file.
// If the given file is a symbol link, it will follow the link.
func IsRegularFile(name string) bool {
	f, e := os.Stat(name)
	if e != nil {
		return false
	}

	return f.Mode().IsRegular()
}

// Md5Sum generates md5 for a given file.
func Md5Sum(name string) string {
	if !IsRegularFile(name) {
		return ""
	}
	f, err := os.Open(name)
	if err != nil {
		return ""
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 8*1024*1024)
	h := md5.New()

	_, err = io.Copy(h, r)
	if err != nil {
		return ""
	}

	return GetMd5Sum(h, nil)
}

// GetMd5Sum gets md5 sum as a string and appends the current hash to b.
func GetMd5Sum(md5 hash.Hash, b []byte) string {
	return fmt.Sprintf("%x", md5.Sum(b))
}

// GetSys returns the underlying data source of the os.FileInfo.
func GetSys(info os.FileInfo) (*syscall.Stat_t, bool) {
	sys, ok := info.Sys().(*syscall.Stat_t)
	return sys, ok
}

// LoadYaml loads yaml config file.
func LoadYaml(path string, out interface{}) error {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to load yaml %s when reading file: %v", path, err)
	}
	if err = yaml.Unmarshal(content, out); err != nil {
		return fmt.Errorf("failed to load yaml %s: %v", path, err)
	}
	return nil
}

func FreeSpace(diskPath string) (Fsize, error) {
	fs := &syscall.Statfs_t{}
	if err := syscall.Statfs(diskPath, fs); err != nil {
		return ToFsize(0), err
	}

	return ToFsize(int64(fs.Bavail) * int64(fs.Bsize)), nil
}

func EmptyDir(path string) (bool, error) {
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
