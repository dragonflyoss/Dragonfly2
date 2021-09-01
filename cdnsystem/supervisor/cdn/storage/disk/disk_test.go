package disk

import (
	"fmt"
	"testing"

	"d7y.io/dragonfly/v2/cdnsystem/storedriver"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor/cdn/storage"
	"d7y.io/dragonfly/v2/cdnsystem/supervisor/mock"
	"github.com/golang/mock/gomock"

	"d7y.io/dragonfly/v2/pkg/unit"

	"github.com/stretchr/testify/suite"
)

func TestDiskStorageMgrSuite(t *testing.T) {
	suite.Run(t, new(DiskStorageMgrSuite))
}

type DiskStorageMgrSuite struct {
	m *diskStorageMgr
	suite.Suite
}

func (suite *DiskStorageMgrSuite) TestTryFreeSpace() {
	ctrl := gomock.NewController(suite.T())
	diskDriver := storedriver.NewMockDriver(ctrl)
	taskMgr := mock.NewMockSeedTaskMgr(ctrl)
	suite.m = &diskStorageMgr{
		diskDriver: diskDriver,
		taskMgr:    taskMgr,
	}
	diskDriver.EXPECT().GetTotalSpace().Return(100*unit.GB, nil)
	cleaner, _ := storage.NewStorageCleaner(suite.m.getDefaultGcConfig(), diskDriver, suite.m, taskMgr)
	suite.m.cleaner = cleaner

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
