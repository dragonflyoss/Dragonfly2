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

package filerw

import (
	"io"
	"os"

	"d7y.io/dragonfly.v2/pkg/util/fileutils"
	"github.com/pkg/errors"
)

// CopyFile copies the file, not dir, src to dst.
func CopyFile(src, dst string) (written int64, err error) {
	var (
		s *os.File
		d *os.File
	)

	if !fileutils.IsRegular(src) {
		return 0, errors.Errorf("copy %s to %s: src is not a regular file", src, dst)
	}

	if fileutils.IsDir(dst) {
		return 0, errors.Errorf("copy %s to %s: dst is a directory", src, dst)
	}

	if s, err = os.Open(src); err != nil {
		return 0, errors.Wrapf(err, "failed to copy %s to %s", src, dst)
	}
	defer s.Close()

	if d, err = fileutils.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return 0, errors.Wrapf(err, "failed to copy %s to %s", src, dst)
	}
	defer d.Close()

	return io.Copy(d, s)
}

// MoveFile moves the file, not dir, src to dst.
func MoveFile(src, dst string) error {
	if !fileutils.IsRegular(src) {
		return errors.Errorf("move %s to %s: src is not a regular file", src, dst)
	}

	var err error
	if err = os.Rename(src, dst); err != nil {
		if _, err = CopyFile(src, dst); err == nil {
			fileutils.DeleteFile(src)
		}
	}

	return errors.Wrapf(err, "failed to move %s to %s", src, dst)
}

// CleanFile cleans content of the file.
func CleanFile(path string) error {
	return os.Truncate(path, 0)
}
