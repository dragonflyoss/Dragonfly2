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

package local

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	cdnerrors "d7y.io/dragonfly/v2/cdn/errors"
	"d7y.io/dragonfly/v2/cdn/storedriver"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/synclock"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
	"d7y.io/dragonfly/v2/pkg/util/statutils"
)

// Ensure driver implements the storedriver.Driver interface
var _ storedriver.Driver = (*driver)(nil)

const (
	DiskDriverName   = "disk"
	MemoryDriverName = "memory"
)

var fileLocker = synclock.NewLockerPool()

func init() {
	if err := storedriver.Register(DiskDriverName, NewStorageDriver); err != nil {
		logger.CoreLogger.Error(err)
	}

	if err := storedriver.Register(MemoryDriverName, NewStorageDriver); err != nil {
		logger.CoreLogger.Error(err)
	}
}

// driver is one of the implementations of storage Driver using local file system.
type driver struct {
	// BaseDir is the dir that local storage driver will store content based on it.
	BaseDir string
}

// NewStorageDriver performs initialization for disk Storage and return a storage Driver.
func NewStorageDriver(cfg *storedriver.Config) (storedriver.Driver, error) {
	return &driver{
		BaseDir: cfg.BaseDir,
	}, nil
}

func (ds *driver) GetTotalSpace() (unit.Bytes, error) {
	path := ds.BaseDir
	lock(path, -1, true)
	defer unLock(path, -1, true)
	return fileutils.GetTotalSpace(path)
}

func (ds *driver) GetHomePath() string {
	return ds.BaseDir
}

func (ds *driver) CreateBaseDir() error {
	return os.MkdirAll(ds.BaseDir, os.ModePerm)
}

func (ds *driver) MoveFile(src string, dst string) error {
	return fileutils.MoveFile(src, dst)
}

// Get the content of key from storage and return in io stream.
func (ds *driver) Get(raw *storedriver.Raw) (io.ReadCloser, error) {
	path, info, err := ds.statPath(raw.Bucket, raw.Key)
	if err != nil {
		return nil, err
	}

	if err := storedriver.CheckGetRaw(raw, info.Size()); err != nil {
		return nil, err
	}

	r, w := io.Pipe()
	go func(w *io.PipeWriter) {
		defer w.Close()

		lock(path, raw.Offset, true)
		defer unLock(path, raw.Offset, true)

		f, err := os.Open(path)
		if err != nil {
			return
		}
		defer func() {
			if err := f.Close(); err != nil {
				logger.Error("close file %s: %v", f, err)
			}
		}()

		if _, err := f.Seek(raw.Offset, io.SeekStart); err != nil {
			logger.Errorf("seek file %s: %v", f, err)
		}
		var reader io.Reader
		reader = f
		if raw.Length > 0 {
			reader = io.LimitReader(f, raw.Length)
		}
		buf := make([]byte, 256*1024)
		if _, err := io.CopyBuffer(w, reader, buf); err != nil {
			logger.Errorf("copy buffer from file %s: %v", f, err)
		}
	}(w)
	return r, nil
}

// GetBytes gets the content of key from storage and return in bytes.
func (ds *driver) GetBytes(raw *storedriver.Raw) (data []byte, err error) {
	path, info, err := ds.statPath(raw.Bucket, raw.Key)
	if err != nil {
		return nil, err
	}

	if err := storedriver.CheckGetRaw(raw, info.Size()); err != nil {
		return nil, err
	}

	lock(path, raw.Offset, true)
	defer unLock(path, raw.Offset, true)

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Errorf("failed to close file %s: %v", f, err)
		}
	}()

	if _, err := f.Seek(raw.Offset, io.SeekStart); err != nil {
		return nil, err
	}
	if raw.Length == 0 {
		data, err = ioutil.ReadAll(f)
	} else {
		data = make([]byte, raw.Length)
		_, err = f.Read(data)
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Put reads the content from reader and put it into storage.
func (ds *driver) Put(raw *storedriver.Raw, data io.Reader) error {
	if err := storedriver.CheckPutRaw(raw); err != nil {
		return err
	}

	path, err := ds.preparePath(raw.Bucket, raw.Key)
	if err != nil {
		return err
	}

	if data == nil {
		return nil
	}

	lock(path, raw.Offset, false)
	defer unLock(path, raw.Offset, false)

	var f *os.File
	if raw.Trunc {
		if err = storedriver.CheckTrunc(raw); err != nil {
			return err
		}
		if f, err = fileutils.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644); err != nil {
			return err
		}
	} else if raw.Append {
		if f, err = fileutils.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644); err != nil {
			return err
		}
	} else {
		if f, err = fileutils.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
			return err
		}
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Errorf("failed to close file %s: %v", f, err)
		}
	}()
	if raw.Trunc {
		if err = f.Truncate(raw.TruncSize); err != nil {
			return err
		}
	}
	if _, err := f.Seek(raw.Offset, io.SeekStart); err != nil {
		return err
	}
	if raw.Length > 0 {
		if _, err = io.CopyN(f, data, raw.Length); err != nil {
			return err
		}
		return nil
	}

	buf := make([]byte, 256*1024)
	if _, err = io.CopyBuffer(f, data, buf); err != nil {
		return err
	}

	return nil
}

// PutBytes puts the content of key from storage with bytes.
func (ds *driver) PutBytes(raw *storedriver.Raw, data []byte) error {
	if err := storedriver.CheckPutRaw(raw); err != nil {
		return err
	}

	path, err := ds.preparePath(raw.Bucket, raw.Key)
	if err != nil {
		return err
	}

	lock(path, raw.Offset, false)
	defer unLock(path, raw.Offset, false)

	var f *os.File
	if raw.Trunc {
		f, err = fileutils.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	} else if raw.Append {
		f, err = fileutils.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	} else {
		f, err = fileutils.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	}
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Errorf("failed to close file %s: %v", f, err)
		}
	}()
	if raw.Trunc {
		if err = f.Truncate(raw.TruncSize); err != nil {
			return err
		}
	}
	if _, err := f.Seek(raw.Offset, io.SeekStart); err != nil {
		return err
	}
	if raw.Length > 0 {
		if _, err := f.Write(data[:raw.Length]); err != nil {
			return err
		}
		return nil
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return nil
}

// Stat determines whether the file exists.
func (ds *driver) Stat(raw *storedriver.Raw) (*storedriver.StorageInfo, error) {
	_, fileInfo, err := ds.statPath(raw.Bucket, raw.Key)
	if err != nil {
		return nil, err
	}
	return &storedriver.StorageInfo{
		Path:       filepath.Join(raw.Bucket, raw.Key),
		Size:       fileInfo.Size(),
		CreateTime: statutils.Ctime(fileInfo),
		ModTime:    fileInfo.ModTime(),
	}, nil
}

// Exits if filepath exists, include symbol link
func (ds *driver) Exits(raw *storedriver.Raw) bool {
	filePath := filepath.Join(ds.BaseDir, raw.Bucket, raw.Key)
	return fileutils.PathExist(filePath)
}

// Remove delete a file or dir.
// It will force delete the file or dir when the raw.Trunc is true.
func (ds *driver) Remove(raw *storedriver.Raw) error {
	path, info, err := ds.statPath(raw.Bucket, raw.Key)
	if err != nil {
		return err
	}

	lock(path, -1, false)
	defer unLock(path, -1, false)

	if raw.Trunc || !info.IsDir() {
		return os.RemoveAll(path)
	}
	empty, err := fileutils.IsEmptyDir(path)
	if empty {
		return os.RemoveAll(path)
	}
	return err
}

// GetFreeSpace returns the available disk space in Byte.
func (ds *driver) GetFreeSpace() (unit.Bytes, error) {
	path := ds.BaseDir
	lock(path, -1, true)
	defer unLock(path, -1, true)
	return fileutils.GetFreeSpace(path)
}

func (ds *driver) GetTotalAndFreeSpace() (unit.Bytes, unit.Bytes, error) {
	path := ds.BaseDir
	lock(path, -1, true)
	defer unLock(path, -1, true)
	return fileutils.GetTotalAndFreeSpace(path)
}

// Walk walks the file tree rooted at root which determined by raw.Bucket and raw.Key,
// calling walkFn for each file or directory in the tree, including root.
func (ds *driver) Walk(raw *storedriver.Raw) error {
	path, _, err := ds.statPath(raw.Bucket, raw.Key)
	if err != nil {
		return err
	}

	lock(path, -1, true)
	defer unLock(path, -1, true)

	return filepath.Walk(path, raw.WalkFn)
}

func (ds *driver) GetPath(raw *storedriver.Raw) string {
	return filepath.Join(ds.BaseDir, raw.Bucket, raw.Key)
}

// helper function

// preparePath gets the target path and creates the upper directory if it does not exist.
func (ds *driver) preparePath(bucket, key string) (string, error) {
	dir := filepath.Join(ds.BaseDir, bucket)
	if err := fileutils.MkdirAll(dir); err != nil {
		return "", err
	}
	target := filepath.Join(dir, key)
	return target, nil
}

// statPath determines whether the target file exists and returns an fileMutex if so.
func (ds *driver) statPath(bucket, key string) (string, os.FileInfo, error) {
	filePath := filepath.Join(ds.BaseDir, bucket, key)
	f, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil, cdnerrors.ErrFileNotExist{File: "filePath"}
		}
		return "", nil, err
	}
	return filePath, f, nil
}

func lock(path string, offset int64, ro bool) {
	if offset != -1 {
		fileLocker.Lock(LockKey(path, -1), true)
	}

	fileLocker.Lock(LockKey(path, offset), ro)
}

func unLock(path string, offset int64, ro bool) {
	if offset != -1 {
		fileLocker.UnLock(LockKey(path, -1), true)
	}

	fileLocker.UnLock(LockKey(path, offset), ro)
}

func LockKey(path string, offset int64) string {
	return fmt.Sprintf("%s:%d", path, offset)
}
