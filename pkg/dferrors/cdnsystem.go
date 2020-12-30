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

package dferrors

var (
	// ErrSystemError represents the error is a system error.
	ErrSystemError = DfError{codeSystemError, "system error"}

	// ErrCDNFail represents the cdn status is fail.
	ErrCDNFail = DfError{codeCDNFail, "cdn status is fail"}

	// ErrCDNWait represents the cdn status is wait.
	ErrCDNWait = DfError{codeCDNWait, "cdn status is wait"}

	// ErrUnknownError represents the error should not happen
	// and the cause of that is unknown.
	ErrUnknownError = DfError{codeUnknownError, "unknown error"}

	// ErrURLNotReachable represents the url is a not reachable.
	ErrURLNotReachable = DfError{codeURLNotReachable, "url not reachable"}

	// ErrTaskIDDuplicate represents the task id is in conflict.
	ErrTaskIDDuplicate = DfError{codeTaskIDDuplicate, "taskId conflict"}

	// ErrAuthenticationRequired represents the authentication is required.
	ErrAuthenticationRequired = DfError{codeAuthenticationRequired, "authentication required"}

	// ErrPieceCountNotEqual
	ErrPieceCountNotEqual = DfError{codePieceCountNotEqual, "inconsistent number of pieces"}

	// ErrFileLengthNotEqual
	ErrFileLengthNotEqual = DfError{codeFileLengthNotEqual, "inconsistent file length"}

	// ErrDownloadFail
	ErrDownloadFail = DfError{codeDownloadFail, "download failed"}

	// ErrResourceExpired
	ErrResourceExpired = DfError{codeResourceExpired, "resource expired"}

	// ErrResourceNotSupportRangeRequest
	ErrResourceNotSupportRangeRequest = DfError{codeResourceNotSupportRangeRequest, "resource does not support range request"}

	// ErrPieceMd5CheckFail
	ErrPieceMd5CheckFail = DfError{codePieceMd5CheckFail, "piece md5 check fail"}
)

const (
	codeSystemError = iota + 1000
	codeCDNFail
	codeCDNWait
	codeUnknownError
	codeURLNotReachable
	codeTaskIDDuplicate
	codeAuthenticationRequired
	codePieceCountNotEqual
	codeFileLengthNotEqual
	codeDownloadFail
	codeResourceExpired
	codeResourceNotSupportRangeRequest
	codePieceMd5CheckFail
)

// IsSystemError checks the error is a system error or not.
func IsSystemError(err error) bool {
	return checkError(err, codeSystemError)
}

// IsCDNFail checks the error is CDNFail or not.
func IsCDNFail(err error) bool {
	return checkError(err, codeCDNFail)
}

// IsCDNWait checks the error is CDNWait or not.
func IsCDNWait(err error) bool {
	return checkError(err, codeCDNWait)
}

// IsUnknownError checks the error is UnknownError or not.
func IsUnknownError(err error) bool {
	return checkError(err, codeUnknownError)
}

// IsURLNotReachable checks the error is a url not reachable or not.
func IsURLNotReachable(err error) bool {
	return checkError(err, codeURLNotReachable)
}

// IsTaskIDDuplicate checks the error is a TaskIDDuplicate error or not.
func IsTaskIDDuplicate(err error) bool {
	return checkError(err, codeTaskIDDuplicate)
}

// IsAuthenticationRequired checks the error is an AuthenticationRequired error or not.
func IsAuthenticationRequired(err error) bool {
	return checkError(err, codeAuthenticationRequired)
}
