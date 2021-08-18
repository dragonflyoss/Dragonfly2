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
//go:generate mockgen -destination ./mock_driver.go -package storedriver d7y.io/dragonfly/v2/cdnsystem/storedriver Driver

package storedriver

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	cdnerrors "d7y.io/dragonfly/v2/cdnsystem/errors"
	"d7y.io/dragonfly/v2/cdnsystem/plugins"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

// Driver defines an interface to manage the data stored in the driver.
//
// NOTE:
// It is recommended that the lock granularity of the driver should be in piece.
// That means that the storage driver could read and write
// the different pieces of the same file concurrently.
type Driver interface {

	// Get data from the storage based on raw information.
	// If the length<=0, the driver should return all data from the raw.offset.
	// Otherwise, just return the data which starts from raw.offset and the length is raw.length.
	Get(raw *Raw) (io.ReadCloser, error)

	// Get data from the storage based on raw information.
	// The data should be returned in bytes.
	// If the length<=0, the storage driver should return all data from the raw.offset.
	// Otherwise, just return the data which starts from raw.offset and the length is raw.length.
	GetBytes(raw *Raw) ([]byte, error)

	// Put the data into the storage with raw information.
	// The storage will get data from io.Reader as io stream.
	// If the offset>0, the storage driver should starting at byte raw.offset off.
	Put(raw *Raw, data io.Reader) error

	// PutBytes puts the data into the storage with raw information.
	// The data is passed in bytes.
	// If the offset>0, the storage driver should starting at byte raw.offset off.
	PutBytes(raw *Raw, data []byte) error

	// Remove the data from the storage based on raw information.
	Remove(raw *Raw) error

	// Stat determines whether the data exists based on raw information.
	// If that, and return some info that in the form of struct StorageInfo.
	// If not, return the ErrFileNotExist.
	Stat(raw *Raw) (*StorageInfo, error)

	// GetFreeSpace returns the available disk space in B.
	GetFreeSpace() (unit.Bytes, error)

	// GetTotalAndFreeSpace
	GetTotalAndFreeSpace() (unit.Bytes, unit.Bytes, error)

	// GetTotalSpace
	GetTotalSpace() (unit.Bytes, error)

	// Walk walks the file tree rooted at root which determined by raw.Bucket and raw.Key,
	// calling walkFn for each file or directory in the tree, including root.
	Walk(raw *Raw) error

	// CreateBaseDir
	CreateBaseDir() error

	// GetPath
	GetPath(raw *Raw) string

	// MoveFile
	MoveFile(src string, dst string) error

	// Exits
	Exits(raw *Raw) bool

	// GetHomePath
	GetHomePath() string
}

type Config struct {
	BaseDir string `yaml:"baseDir"`
}

// Raw identifies a piece of data uniquely.
// If the length<=0, it represents all data.
type Raw struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	Offset    int64  `json:"offset"`
	Length    int64  `json:"length"`
	Trunc     bool   `json:"trunc"`
	TruncSize int64  `json:"trunc_size"`
	Append    bool   `json:"append"`
	WalkFn    filepath.WalkFunc
}

// StorageInfo includes partial meta information of the data.
type StorageInfo struct {
	Path       string    `json:"path"`        // file path
	Size       int64     `json:"size"`        // file size
	CreateTime time.Time `json:"create_time"` // create time
	ModTime    time.Time `json:"mod_time"`    // modified time
}

// driverPlugin is a wrapper of the storage driver which implements the interface of Driver.
type driverPlugin struct {
	// name is a unique identifier, you can also name it ID.
	name string
	// instance holds a storage which implements the interface of driverPlugin.
	instance Driver
}

// Ensure that driverPlugin implements the interface of Driver
var _ Driver = (*driverPlugin)(nil)

// Ensure that driverPlugin implements the interface plugins.Plugin
var _ plugins.Plugin = (*driverPlugin)(nil)

// NewDriverPlugin creates a new storage Driver Plugin instance.
func newDriverPlugin(name string, builder DriverBuilder, cfg *Config) (plugins.Plugin, error) {
	if name == "" || builder == nil {
		return nil, fmt.Errorf("storage driver plugin's name and builder cannot be nil")
	}
	// init driver with specific config
	driver, err := builder(cfg)
	if err != nil {
		return nil, fmt.Errorf("init storage driver %s: %v", name, err)
	}

	return &driverPlugin{
		name:     name,
		instance: driver,
	}, nil
}

// Type returns the plugin type StorageDriverPlugin.
func (s *driverPlugin) Type() plugins.PluginType {
	return plugins.StorageDriverPlugin
}

// Name returns the plugin name.
func (s *driverPlugin) Name() string {
	return s.name
}

// GetTotalSpace
func (s *driverPlugin) GetTotalSpace() (unit.Bytes, error) {
	return s.instance.GetTotalSpace()
}

// CreateBaseDir
func (s *driverPlugin) CreateBaseDir() error {
	return s.instance.CreateBaseDir()
}

func (s *driverPlugin) Exits(raw *Raw) bool {
	return s.instance.Exits(raw)
}

func (s *driverPlugin) GetTotalAndFreeSpace() (unit.Bytes, unit.Bytes, error) {
	return s.instance.GetTotalAndFreeSpace()
}

// Get the data from the storage driver in io stream.
func (s *driverPlugin) Get(raw *Raw) (io.ReadCloser, error) {
	if err := checkEmptyKey(raw); err != nil {
		return nil, err
	}
	return s.instance.Get(raw)
}

// GetBytes gets the data from the storage driver in bytes.
func (s *driverPlugin) GetBytes(raw *Raw) ([]byte, error) {
	if err := checkEmptyKey(raw); err != nil {
		return nil, err
	}
	return s.instance.GetBytes(raw)
}

// Put puts data into the storage in io stream.
func (s *driverPlugin) Put(raw *Raw, data io.Reader) error {
	if err := checkEmptyKey(raw); err != nil {
		return err
	}
	return s.instance.Put(raw, data)
}

// PutBytes puts data into the storage in bytes.
func (s *driverPlugin) PutBytes(raw *Raw, data []byte) error {
	if err := checkEmptyKey(raw); err != nil {
		return err
	}
	return s.instance.PutBytes(raw, data)
}

// AppendBytes append data into storage in bytes.
//func (s *Store) AppendBytes(ctx context.Context, raw *Raw, data []byte) error {
//	if err := checkEmptyKey(raw); err != nil {
//		return err
//	}
//	return s.driver.AppendBytes(ctx, raw, data)
//}

// Remove the data from the storage based on raw information.
func (s *driverPlugin) Remove(raw *Raw) error {
	if raw == nil || (stringutils.IsBlank(raw.Key) &&
		stringutils.IsBlank(raw.Bucket)) {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "cannot set both key and bucket empty at the same time")
	}
	return s.instance.Remove(raw)
}

// Stat determines whether the data exists based on raw information.
// If that, and return some info that in the form of struct StorageInfo.
// If not, return the ErrNotFound.
func (s *driverPlugin) Stat(raw *Raw) (*StorageInfo, error) {
	if err := checkEmptyKey(raw); err != nil {
		return nil, err
	}
	return s.instance.Stat(raw)
}

// Walk walks the file tree rooted at root which determined by raw.Bucket and raw.Key,
// calling walkFn for each file or directory in the tree, including root.
func (s *driverPlugin) Walk(raw *Raw) error {
	return s.instance.Walk(raw)
}

func (s *driverPlugin) GetPath(raw *Raw) string {
	return s.instance.GetPath(raw)
}

func (s *driverPlugin) MoveFile(src string, dst string) error {
	return s.instance.MoveFile(src, dst)
}

// GetFreeSpace returns the available disk space in B.
func (s *driverPlugin) GetFreeSpace() (unit.Bytes, error) {
	return s.instance.GetFreeSpace()
}

func (s *driverPlugin) GetHomePath() string {
	return s.instance.GetHomePath()
}

func checkEmptyKey(raw *Raw) error {
	if raw == nil || stringutils.IsBlank(raw.Key) {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "raw key is empty")
	}
	return nil
}
