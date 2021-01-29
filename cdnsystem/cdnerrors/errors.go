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

package cdnerrors

import (
	"github.com/pkg/errors"
)

var (
	// ErrSystemError represents the error is a system error.
	ErrSystemError = errors.New("system error")

	// ErrCDNFail represents the cdn status is fail.
	ErrCDNFail = errors.New("cdn status is fail")

	// ErrCDNWait represents the cdn status is wait.
	ErrCDNWait = errors.New("cdn status is wait")

	// ErrURLNotReachable represents the url is a not reachable.
	ErrURLNotReachable = errors.New("url not reachable")

	// ErrTaskIDDuplicate represents the task id is in conflict.
	ErrTaskIDDuplicate = errors.New("taskId conflict")

	// ErrAuthenticationRequired represents the authentication is required.
	ErrAuthenticationRequired = errors.New("authentication required")

	// ErrPieceCountNotEqual
	ErrPieceCountNotEqual = errors.New("inconsistent number of pieces")

	// ErrFileLengthNotEqual
	ErrFileLengthNotEqual = errors.New("inconsistent file length")

	// ErrDownloadFail
	ErrDownloadFail = errors.New("download failed")

	// ErrResourceExpired
	ErrResourceExpired = errors.New("resource expired")

	// ErrResourceNotSupportRangeRequest
	ErrResourceNotSupportRangeRequest = errors.New("resource does not support range request")

	// ErrPieceMd5CheckFail
	ErrPieceMd5CheckFail = errors.New("piece md5 check fail")

	// ErrDataNotFound represents the data cannot be found.
	ErrDataNotFound = errors.New("data not found")

	// ErrEmptyValue represents the value is empty or nil.
	ErrEmptyValue = errors.New("empty value")

	// ErrInvalidValue represents the value is invalid.
	ErrInvalidValue = errors.New("invalid value")

	// ErrNotInitialized represents the object is not initialized.
	ErrNotInitialized = errors.New("not initialized")

	// ErrConvertFailed represents failed to convert.
	ErrConvertFailed = errors.New("convert failed")

	// ErrRangeNotSatisfiable represents the length of file is insufficient.
	ErrRangeNotSatisfiable = errors.New("range not satisfiable")

	// ErrEndOfStream represents end of stream
	ErrEndOfStream = errors.New("end of stream")

	// ErrAddressReused represents address is reused
	ErrAddressReused = errors.New("address is reused")

	// ErrUnknownError represents the error should not happen and the cause of that is unknown.
	ErrUnknownError = errors.New("unknown error")

)

// IsSystemError checks the error is a system error or not.
func IsSystemError(err error) bool {
	return errors.Cause(err) == ErrSystemError
}

// IsCDNFail checks the error is CDNFail or not.
func IsCDNFail(err error) bool {
	return errors.Cause(err) == ErrCDNFail
}

// IsCDNWait checks the error is CDNWait or not.
func IsCDNWait(err error) bool {
	return errors.Cause(err) == ErrCDNWait
}

// IsURLNotReachable checks the error is a url not reachable or not.
func IsURLNotReachable(err error) bool {
	return errors.Cause(err) == ErrURLNotReachable
}

// IsTaskIDDuplicate checks the error is a TaskIDDuplicate error or not.
func IsTaskIDDuplicate(err error) bool {
	return errors.Cause(err) == ErrTaskIDDuplicate
}

// IsAuthenticationRequired checks the error is an AuthenticationRequired error or not.
func IsAuthenticationRequired(err error) bool {
	return errors.Cause(err) == ErrAuthenticationRequired
}

func IsDataNotFound(err error) bool {
	return errors.Cause(err) == ErrDataNotFound
}

// IsUnknownError checks the error is UnknownError or not.
func IsUnknownError(err error) bool {
	return errors.Cause(err) == ErrUnknownError
}
