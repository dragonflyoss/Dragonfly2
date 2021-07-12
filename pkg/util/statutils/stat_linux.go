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

package statutils

import (
	"os"
	"syscall"
	"time"

	"d7y.io/dragonfly.v2/pkg/unit"
)

// Atime returns the last access time in time.Time.
func Atime(info os.FileInfo) time.Time {
	stat := GetSysStat(info)
	return time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
}

// AtimeSec returns the last access time in seconds.
func AtimeSec(info os.FileInfo) int64 {
	stat := GetSysStat(info)
	return stat.Atim.Sec
}

// Ctime returns the create time in time.Time.
func Ctime(info os.FileInfo) time.Time {
	stat := GetSysStat(info)
	return time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)
}

// CtimeSec returns the create time in seconds.
func CtimeSec(info os.FileInfo) int64 {
	stat := GetSysStat(info)
	return stat.Ctim.Sec
}

// GetSysStat returns underlying data source of the os.FileInfo.
func GetSysStat(info os.FileInfo) *syscall.Stat_t {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return stat
	}
	return nil
}

func FreeSpace(diskPath string) (unit.Bytes, error) {
	fs := &syscall.Statfs_t{}
	if err := syscall.Statfs(diskPath, fs); err != nil {
		return 0, err
	}

	return unit.ToBytes(int64(fs.Bavail) * fs.Bsize), nil
}
