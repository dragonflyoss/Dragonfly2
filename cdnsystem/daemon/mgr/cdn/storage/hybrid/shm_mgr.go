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

package hybrid

import (
	"context"
	"d7y.io/dragonfly/v2/cdnsystem/store"
	"d7y.io/dragonfly/v2/pkg/util/fileutils"
)

const SecureLevel = 500 * 1024 * 1024

type ShareMemManager struct {
	switcher     shmSwitcher
	spaceGcTimer shmSpaceGcTimer
	memoryStore  store.StorageDriver
}

func newShareMemManager() *ShareMemManager {
	return &ShareMemManager{
		switcher:     shmSwitcher{},
		spaceGcTimer: shmSpaceGcTimer{},
	}
}
func (sm *ShareMemManager) tryShmSpace(ctx context.Context, url, taskId string, fileLength int64) (string, error) {
	if sm.switcher.check(url, fileLength) && sm.spaceGcTimer.canUseShm() {
		//remainder := atomic.NewInt64(0)
		//sm.memoryStore.Walk(ctx, )
		//filepath.Walk(config.ShmHome, func(path string, info os.FileInfo, err error) error {
		//	task := &types.SeedTask{}
		//	var totalLen int64 = 0
		//	if task.CdnFileLength > 0 {
		//		totalLen = task.CdnFileLength
		//	} else {
		//		totalLen = task.SourceFileLength
		//	}
		//	if totalLen > 0 {
		//		remainder.Add(
		//			totalLen - info.Size())
		//	}
		//	return nil
		//})
		//
		//usableSpace, err := sm.getUsableSpace()
		//if err != nil {
		//
		//}
		//useShm := int64(usableSpace)-remainder.Load()-SecureLevel >= fileLength
		//if !useShm {
		//	// 如果剩余空间过小，则强制执行一次fullgc后在检查是否满足
		//	sm.spaceGcTimer.force()
		//	useShm = int64(usableSpace)-remainder.Load()-SecureLevel >= fileLength
		//}
		//if useShm { // 创建shm
		//	return "", nil
		//}
	}
	return "", nil
}

/**
 * 获取可用空间
 * @return
 */
func (sm *ShareMemManager) getUsableSpace() (fileutils.Fsize, error) {
	//totalSize, freeSize, err :=sm.memoryStore.GetAvailSpace()
	//if err != nil {
	//	return 0, err
	//}
	//// 如果文件所在分区的总容量大于等于 72G，则返回磁盘的剩余可用空间
	//threshold := 72 * 1024 * 1024 * 1024
	//if totalSize >= fileutils.Fsize(threshold) {
	//	return freeSize, nil
	//}
	//// 如果总容量小于72G， 如40G容量，则可用空间为 当前可用空间 - 32G： 最大可用空间为8G，50G容量，则可用空间为 当前可用空间 - 22G：最大可用空间为28G
	//
	//usableSpace := freeSize - (72*1024*1024*1024 - totalSize)
	//if usableSpace > 0 {
	//	return usableSpace, nil
	//}
	return 0, nil
}
