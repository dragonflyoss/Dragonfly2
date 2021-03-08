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

	"d7y.io/dragonfly/v2/pkg/ratelimiter"
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
	DefaultYamlConfigFile = "/etc/dragonfly/dfget.yml"
	ProxyYamlConfigFile   = "/etc/dragonfly/proxy.yml"
	DefaultLocalLimit     = 20 * ratelimiter.MB
	DefaultMinRate        = 64 * ratelimiter.KB
)

/* http headers */
const (
	StrRange         = "Range"
	StrContentLength = "Content-Length"
	StrContentType   = "Content-Type"
	StrUserAgent     = "User-Agent"

	StrTaskFileName = "taskFileName"
	StrClientID     = "cid"
	StrTaskID       = "taskID"
	StrSuperNode    = "superNode"
	StrRateLimit    = "rateLimit"
	StrPieceNum     = "pieceNum"
	StrPieceSize    = "pieceSize"
	StrDataDir      = "dataDir"
	StrTotalLimit   = "totalLimit"
	StrCDNSource    = "cdnSource"

	StrBytes   = "bytes"
	StrPattern = "pattern"
)

/* others */
const (
	DefaultTimestampFormat = "2006-01-02 15:04:05"
	SchemaHTTP             = "http"

	ServerPortLowerLimit = 15000
	ServerPortUpperLimit = 65000

	RangeNotSatisfiableDesc = "range not satisfiable"
	AddrUsedDesc            = "address already in use"

	PeerHTTPPathPrefix = "/peer/file/"
	CDNPathPrefix      = "/qtdown/"

	LocalHTTPPathCheck  = "/check/"
	LocalHTTPPathClient = "/client/"
	LocalHTTPPathRate   = "/rate/"
	LocalHTTPPing       = "/server/ping"

	DataExpireTime         = 3 * time.Minute
	DaemonAliveTime        = 5 * time.Minute
	DefaultDownloadTimeout = 5 * time.Minute

	DefaultSupernodeSchema = "http"
	DefaultSupernodeIP     = "127.0.0.1"
	DefaultSupernodePort   = 8002
)

/* errors code */
const (
	// CodeLaunchServerError represents failed to launch a peer server.
	CodeLaunchServerError = 1100 + iota

	// CodePrepareError represents failed to prepare before downloading.
	CodePrepareError

	// CodeGetUserError represents failed to get current user.
	CodeGetUserError

	// CodeRegisterError represents failed to register to supernode.
	CodeRegisterError

	// CodeDownloadError represents failed to download file.
	CodeDownloadError
)

const (
	RangeSeparator = "-"
)
