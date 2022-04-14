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

	"d7y.io/dragonfly/v2/pkg/unit"
)

/* the reason of backing to source */
const (
	BackSourceReasonNone          = 0
	BackSourceReasonRegisterFail  = 1
	BackSourceReasonMd5NotMatch   = 2
	BackSourceReasonDownloadError = 3
	BackSourceReasonNoSpace       = 4
	BackSourceReasonInitError     = 5
	BackSourceReasonWriteError    = 6
	BackSourceReasonHostSysError  = 7
	BackSourceReasonNodeEmpty     = 8
	BackSourceReasonSourceError   = 10
	BackSourceReasonUserSpecified = 100
	ForceNotBackSourceAddition    = 1000
)

/* download pattern */
const (
	PatternP2P    = "p2p"
	PatternCDN    = "cdn"
	PatternSource = "source"
)

const (
	DefaultPerPeerDownloadLimit = 20 * unit.MB
	DefaultTotalDownloadLimit   = 100 * unit.MB
	DefaultUploadLimit          = 100 * unit.MB
	DefaultMinRate              = 64 * unit.KB
)

/* others */
const (
	DefaultTimestampFormat = "2006-01-02 15:04:05"
	SchemaHTTP             = "http"

	ServerPortLowerLimit = 15000
	ServerPortUpperLimit = 65000

	DefaultTaskExpireTime  = 6 * time.Hour
	DefaultGCInterval      = 1 * time.Minute
	DefaultDaemonAliveTime = 5 * time.Minute
	DefaultScheduleTimeout = 5 * time.Minute
	DefaultDownloadTimeout = 5 * time.Minute

	DefaultSchedulerSchema = "http"
	DefaultSchedulerIP     = "127.0.0.1"
	DefaultSchedulerPort   = 8002

	DefaultPieceChanSize = 16
)

const (
	SimpleLocalTaskStoreStrategy  = StoreStrategy("io.d7y.storage.v2.simple")
	AdvanceLocalTaskStoreStrategy = StoreStrategy("io.d7y.storage.v2.advance")
)

/* dfcache subcommand names */
const (
	CmdStat   = "stat"
	CmdImport = "import"
	CmdExport = "export"
	CmdDelete = "delete"
)
