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

type shmSpaceGcTimer struct {
	/**
	 * 基础知识 /dev/shm 的容量默认最大为内存的一半大小
	 * hasShm 为 false 的情况
	 * 1、SHM_HOME 文件不存在
	 * 2、SHM_HOME 所在分区空间 <= 36G
	 */
	hasShm bool
}

func newShmSpaceGc() *shmSpaceGcTimer {
	return &shmSpaceGcTimer{hasShm: true}
}
func (ssGcTimer *shmSpaceGcTimer) canUseShm() bool {
	return ssGcTimer.hasShm
}

func (ssGcTimer *shmSpaceGcTimer) force() {

}