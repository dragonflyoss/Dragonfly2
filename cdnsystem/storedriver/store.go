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

package storedriver

import (
	"context"

	"fmt"
	"io"

	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"d7y.io/dragonfly/v2/cdnsystem/config"
	"d7y.io/dragonfly/v2/pkg/unit"
	"d7y.io/dragonfly/v2/pkg/util/stringutils"
	"github.com/pkg/errors"
)

func init() {
	// Ensure that storage implements the StorageDriver interface
	var storage *Store = nil
	var _ Driver = storage
}

// Store is a wrapper of the storage which implements the interface of StorageDriver.
type Store struct {
	// name is a unique identifier, you can also name it ID.
	driverName string
	// config is used to init storage driver.
	config interface{}
	// driver holds a storage which implements the interface of StorageDriver.
	driver Driver
}

// NewStore creates a new Store instance.
func NewStore(name string, builder StorageBuilder, cfg interface{}) (*Store, error) {
	if name == "" || builder == nil {
		return nil, fmt.Errorf("plugin name or builder cannot be nil")
	}

	// init driver with specific config
	driver, err := builder(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to init storage driver %s: %v", name, err)
	}

	return &Store{
		driverName: name,
		config:     cfg,
		driver:     driver,
	}, nil
}


// Type returns the plugin type: StoragePlugin.
func (s *Store) Type() config.PluginType {
	return config.StoragePlugin
}

// Name returns the plugin name.
func (s *Store) Name() string {
	return s.driverName
}

// GetTotalSpace
func (s *Store) GetTotalSpace(ctx context.Context) (unit.Bytes, error) {
	return s.driver.GetTotalSpace(ctx)
}

// CreateBaseDir
func (s *Store) CreateBaseDir(ctx context.Context) error {
	return s.driver.CreateBaseDir(ctx)
}


func (s *Store) Exits(ctx context.Context, raw *Raw) bool {
	return s.driver.Exits(ctx, raw)
}

func (s *Store) GetTotalAndFreeSpace(ctx context.Context) (unit.Bytes, unit.Bytes, error) {
	return s.driver.GetTotalAndFreeSpace(ctx)
}

// Get the data from the storage driver in io stream.
func (s *Store) Get(ctx context.Context, raw *Raw) (io.ReadCloser, error) {
	if err := checkEmptyKey(raw); err != nil {
		return nil, err
	}
	return s.driver.Get(ctx, raw)
}

// GetBytes gets the data from the storage driver in bytes.
func (s *Store) GetBytes(ctx context.Context, raw *Raw) ([]byte, error) {
	if err := checkEmptyKey(raw); err != nil {
		return nil, err
	}
	return s.driver.GetBytes(ctx, raw)
}

// Put puts data into the storage in io stream.
func (s *Store) Put(ctx context.Context, raw *Raw, data io.Reader) error {
	if err := checkEmptyKey(raw); err != nil {
		return err
	}
	return s.driver.Put(ctx, raw, data)
}

// PutBytes puts data into the storage in bytes.
func (s *Store) PutBytes(ctx context.Context, raw *Raw, data []byte) error {
	if err := checkEmptyKey(raw); err != nil {
		return err
	}
	return s.driver.PutBytes(ctx, raw, data)
}

// AppendBytes append data into storage in bytes.
//func (s *Store) AppendBytes(ctx context.Context, raw *Raw, data []byte) error {
//	if err := checkEmptyKey(raw); err != nil {
//		return err
//	}
//	return s.driver.AppendBytes(ctx, raw, data)
//}

// Remove the data from the storage based on raw information.
func (s *Store) Remove(ctx context.Context, raw *Raw) error {
	if raw == nil || (stringutils.IsBlank(raw.Key) &&
		stringutils.IsBlank(raw.Bucket)) {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "cannot set both key and bucket empty at the same time")
	}
	return s.driver.Remove(ctx, raw)
}

// Stat determines whether the data exists based on raw information.
// If that, and return some info that in the form of struct StorageInfo.
// If not, return the ErrNotFound.
func (s *Store) Stat(ctx context.Context, raw *Raw) (*StorageInfo, error) {
	if err := checkEmptyKey(raw); err != nil {
		return nil, err
	}
	return s.driver.Stat(ctx, raw)
}

// Walk walks the file tree rooted at root which determined by raw.Bucket and raw.Key,
// calling walkFn for each file or directory in the tree, including root.
func (s *Store) Walk(ctx context.Context, raw *Raw) error {
	return s.driver.Walk(ctx, raw)
}

func (s *Store) GetPath(raw *Raw) string {
	return s.driver.GetPath(raw)
}

func (s *Store) MoveFile(src string, dst string) error {
	return s.driver.MoveFile(src, dst)
}

// GetAvailSpace returns the available disk space in B.
func (s *Store) GetAvailSpace(ctx context.Context) (unit.Bytes, error) {
	return s.driver.GetAvailSpace(ctx)
}

func (s *Store) GetHomePath(ctx context.Context) string {
	return s.driver.GetHomePath(ctx)
}

func (s *Store) GetGcConfig(ctx context.Context) *GcConfig {
	return s.driver.GetGcConfig(ctx)
}

func checkEmptyKey(raw *Raw) error {
	if raw == nil || stringutils.IsBlank(raw.Key) {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "raw key is empty")
	}
	return nil
}
