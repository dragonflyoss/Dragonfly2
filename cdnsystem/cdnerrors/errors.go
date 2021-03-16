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

	// ErrTaskIdDuplicate represents the task id is in conflict.
	ErrTaskIdDuplicate = errors.New("taskId conflict")

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

	// ErrKeyNotFound is an error which will be returned when the key can not be found.
	ErrKeyNotFound = errors.New("the key not found")

	// ErrFileNotExist represents the file is not exists
	ErrFileNotExist = errors.New("file not exist")

	// ErrEmptyKey represents the key is empty or nil.
	ErrEmptyKey = errors.New("empty key")

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
	return errors.Cause(err) == ErrTaskIdDuplicate
}

// IsAuthenticationRequired checks the error is an AuthenticationRequired error or not.
func IsAuthenticationRequired(err error) bool {
	return errors.Cause(err) == ErrAuthenticationRequired
}

func IsPieceCountNotEqual(err error) bool {
	return errors.Cause(err) == ErrPieceCountNotEqual
}

func IsFileLengthNotEqual(err error) bool {
	return errors.Cause(err) == ErrFileLengthNotEqual
}

func IsDownloadFail(err error) bool {
	return errors.Cause(err) == ErrDownloadFail
}

func IsResourceExpired(err error) bool {
	return errors.Cause(err) == ErrResourceExpired
}

func IsResourceNotSupportRangeRequest(err error) bool {
	return errors.Cause(err) == ErrResourceNotSupportRangeRequest
}

func IsPieceMd5CheckFail(err error) bool {
	return errors.Cause(err) == ErrPieceMd5CheckFail
}

func IsDataNotFound(err error) bool {
	return errors.Cause(err) == ErrDataNotFound
}

// IsKeyNotFound
func IsKeyNotFound(err error) bool {
	return errors.Cause(err) == ErrKeyNotFound
}

// IsEmptyKey
func IsEmptyKey(err error) bool {
	return errors.Cause(err) == ErrEmptyKey
}

func IsEmptyValue(err error) bool {
	return errors.Cause(err) == ErrEmptyValue
}

func IsInvalidValue(err error) bool {
	return errors.Cause(err) == ErrInvalidValue
}

func IsNotInitialized(err error) bool {
	return errors.Cause(err) == ErrNotInitialized
}

func IsConvertFailed(err error) bool {
	return errors.Cause(err) == ErrConvertFailed
}

func IsRangeNotSatisfiable(err error) bool {
	return errors.Cause(err) == ErrRangeNotSatisfiable
}

func IsAddressReused(err error) bool {
	return errors.Cause(err) == ErrAddressReused
}

// IsUnknownError checks the error is UnknownError or not.
func IsUnknownError(err error) bool {
	return errors.Cause(err) == ErrUnknownError
}

func IsNilError(err error) bool {
	return errors.Cause(err) == nil
}

