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

package config

import (
	"time"

	"github.com/dragonflyoss/Dragonfly2/pkg/rate"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/fileutils"
)

const (
	//CDNPatternLocal
	CDNPatternLocal CDNPattern = "local"
	//CDNPatternSource
	CDNPatternSource CDNPattern = "source"
)

const (
	// RepoHome is the directory where store data
	RepoHome = "repo"
	// DownloadHome is the parent directory where the downloaded files are stored
	// which is a relative path.
	DownloadHome = "download"
)

const (
	// CDNWriterRoutineLimit 4
	CDNWriterRoutineLimit = 4
)

const (
	// SubsystemCdnNode represents metrics from cdn system.
	SubsystemCdnSystem = "cdnSystem"
)

const (
	// DefaultListenPort is the default port cdn server listens on.
	DefaultListenPort = 8002
	// DefaultListenHttpPort is the default port cdn http server listens on.
	//DefaultListenHttpPort = 8002
	// DefaultDownloadPort is the default port for download files from cdn.
	DefaultDownloadPort = 8001
)

const (
	DefaultCDNPattern = CDNPatternLocal
)

const (
	// DefaultCdnConfigFilePath the default cdn config path.
	DefaultCdnConfigFilePath = "/etc/dragonfly/cdn.yml"
)

const (
	// DefaultPieceSize 4M
	DefaultPieceSize = 4 * 1024 * 1024

	// DefaultPieceSizeLimit 15M
	DefaultPieceSizeLimit = 15 * 1024 * 1024
)

const (
	// DefaultFailAccessInterval is the interval time after failed to access the URL.
	DefaultFailAccessInterval = 3 * time.Minute
)

// gc
const (
	// DefaultGCInitialDelay is the delay time from the start to the first GC execution.
	DefaultGCInitialDelay = 6 * time.Second

	// DefaultGCMetaInterval is the interval time to execute the GC meta.
	DefaultGCMetaInterval = 2 * time.Minute

	// DefaultTaskExpireTime when a task is not accessed within the taskExpireTime,
	// and it will be treated to be expired.
	DefaultTaskExpireTime = 3 * time.Minute

	DefaultYoungGCThreshold = 100 * fileutils.GB

	DefaultFullGCThreshold = 5 * fileutils.GB

	DefaultIntervalThreshold = 2 * time.Hour

	DefaultGCDiskInterval = 15 * time.Second

	DefaultCleanRatio = 1
)

const (
	// DefaultSystemReservedBandwidth is the default network bandwidth reserved for system software.
	// unit: MB/s
	DefaultSystemReservedBandwidth = 20 * rate.MB
	// DefaultMaxBandwidth is the default network bandwidth that cdn can use.
	// unit: MB/s
	DefaultMaxBandwidth = 200 * rate.MB
)
