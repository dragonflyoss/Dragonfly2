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

package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"d7y.io/dragonfly/v2/cdn/storedriver/local"
	"d7y.io/dragonfly/v2/cdn/supervisor/task"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"d7y.io/dragonfly/v2/cdn/storedriver"
	"d7y.io/dragonfly/v2/cdn/supervisor/cdn/storage"
	taskMock "d7y.io/dragonfly/v2/cdn/supervisor/mocks/task"
	"d7y.io/dragonfly/v2/pkg/unit"
)

func TestDiskStorageManagerSuite(t *testing.T) {
	suite.Run(t, new(DiskStorageManagerSuite))
}

type DiskStorageManagerSuite struct {
	m *diskStorageManager
	suite.Suite
}

func (suite *DiskStorageManagerSuite) TestTryFreeSpace() {
	ctrl := gomock.NewController(suite.T())
	diskDriver := storedriver.NewMockDriver(ctrl)
	taskManager := taskMock.NewMockManager(ctrl)
	suite.m = &diskStorageManager{
		diskDriver:  diskDriver,
		taskManager: taskManager,
	}
	diskDriver.EXPECT().GetTotalSpace().Return(100*unit.GB, nil)
	diskDriver.EXPECT().GetBaseDir().Return(storage.DefaultDiskBaseDir)
	driverDriverConfig, err := getDiskDriverConfig(diskDriver)
	suite.Nil(err)
	cleaner, err := storage.NewStorageCleaner(driverDriverConfig.DriverGCConfig, diskDriver, suite.m, taskManager)
	suite.Require().Nil(err)
	suite.m.diskCleaner = cleaner

	tests := []struct {
		name       string
		setupSuite func()
		fileLength int64
		success    func(bool, error) bool
	}{
		{
			name: "very large free space",
			setupSuite: func() {
				// call GetFreeSpace 1 time in TryFreeSpace and return
				diskDriver.EXPECT().GetFreeSpace().Return(unit.TB, nil)
			},
			fileLength: unit.MB.ToNumber(),
			success: func(ok bool, err error) bool {
				return ok == true && err == nil
			},
		},
		{
			name: "try a small file",
			setupSuite: func() {
				// call GetFreeSpace 1 time in TryFreeSpace
				diskDriver.EXPECT().GetFreeSpace().Return(100*unit.GB, nil)
				// call Walk 1 time in TryFreeSpace
				diskDriver.EXPECT().Walk(gomock.Any())
			},
			fileLength: unit.KB.ToNumber(),
			success: func(ok bool, err error) bool {
				return ok == true && err == nil
			},
		},
		{
			name: "try a very large file",
			setupSuite: func() {
				// call GetFreeSpace 2 times in TryFreeSpace, 1 time in GC
				diskDriver.EXPECT().GetFreeSpace().Return(100*unit.GB, nil).Times(3)
				// call Walk 2 times in TryFreeSpace, 1 time in GC
				diskDriver.EXPECT().Walk(gomock.Any()).Times(3)
			},
			fileLength: unit.TB.ToNumber(),
			success: func(ok bool, err error) bool {
				return ok == false && err == nil
			},
		},
		{
			name: "if get free space meets error",
			setupSuite: func() {
				// call GetFreeSpace 1 times in TryFreeSpace and return
				diskDriver.EXPECT().GetFreeSpace().Return(unit.ToBytes(0), fmt.Errorf("a error for test"))
			},
			fileLength: unit.MB.ToNumber(),
			success: func(ok bool, err error) bool {
				return ok == false && err != nil && err.Error() == "a error for test"
			},
		},
		{
			name: "ok after gc",
			setupSuite: func() {
				// first call GetFreeSpace 1 times in TryFreeSpace, 1 time in GC
				diskDriver.EXPECT().GetFreeSpace().Return(100*unit.MB, nil).Times(2)
				// then call GetFreeSpace 1 times in TryFreeSpace, get another value
				diskDriver.EXPECT().GetFreeSpace().Return(100*unit.GB, nil)
				// call Walk 2 times in TryFreeSpace, 1 time in GC
				diskDriver.EXPECT().Walk(gomock.Any()).Times(3)
			},
			fileLength: unit.GB.ToNumber(),
			success: func(ok bool, err error) bool {
				return ok == true && err == nil
			},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			tt.setupSuite()
			suite.True(tt.success(suite.m.TryFreeSpace(tt.fileLength)))
		})
	}
}

func (suite *DiskStorageManagerSuite) TestDeleteTask() {
	workHome, _ := os.MkdirTemp("/tmp", "cdn-storageManager-")
	fmt.Printf("workHome: %s", workHome)
	diskDriver, err := local.NewStorageDriver(&storedriver.Config{BaseDir: workHome})
	suite.Nil(err)
	suite.m = &diskStorageManager{
		diskDriver: diskDriver,
	}
	testTask := task.NewSeedTask("fooTask", "http://www.dragonfly", &base.UrlMeta{})
	suite.Nil(suite.m.ResetRepo(testTask))
	downloadFile, _ := os.Stat(filepath.Join(workHome, "download", "foo", "fooTask"))
	uploadFile, _ := os.Stat(filepath.Join(workHome, "upload", "foo", "fooTask"))
	suite.NotNil(downloadFile)
	suite.NotNil(uploadFile)
	suite.m.DeleteTask(testTask.ID)
	downloadFile, _ = os.Stat(filepath.Join(workHome, "download", "foo", "fooTask"))
	uploadFile, _ = os.Stat(filepath.Join(workHome, "upload", "foo", "fooTask"))
	suite.Nil(downloadFile)
	suite.Nil(uploadFile)
	os.RemoveAll(workHome)
}
