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

	// ErrURLNotReachable represents the url is a not reachable.
	ErrURLNotReachable = errors.New("url not reachable")

	// ErrTaskIdDuplicate represents the task id is in conflict.
	ErrTaskIdDuplicate = errors.New("taskId conflict")

	// ErrPieceCountNotEqual
	ErrPieceCountNotEqual = errors.New("inconsistent number of pieces")

	// ErrFileLengthNotEqual
	ErrFileLengthNotEqual = errors.New("inconsistent file length")

	// ErrDownloadFail
	ErrDownloadFail = errors.New("resource download failed")

	// ErrResourceExpired
	ErrResourceExpired = errors.New("resource expired")

	// ErrResourceNotSupportRangeRequest
	ErrResourceNotSupportRangeRequest = errors.New("resource does not support range request")

	// ErrPieceMd5NotMatch
	ErrPieceMd5NotMatch = errors.New("piece md5 check fail")

	// ErrDataNotFound represents the data cannot be found.
	ErrDataNotFound = errors.New("data not found")

	// ErrFileNotExist represents the file is not exists
	ErrFileNotExist = errors.New("file or directory not exist")

	// ErrInvalidValue represents the value is invalid.
	ErrInvalidValue = errors.New("invalid value")

	// ErrConvertFailed represents failed to convert.
	ErrConvertFailed = errors.New("convert failed")

	// ErrRangeNotSatisfiable represents the length of file is insufficient.
	ErrRangeNotSatisfiable = errors.New("range not satisfiable")
)

// IsSystemError checks the error is a system error or not.
func IsSystemError(err error) bool {
	return errors.Cause(err) == ErrSystemError
}

// IsURLNotReachable checks the error is a url not reachable or not.
func IsURLNotReachable(err error) bool {
	return errors.Cause(err) == ErrURLNotReachable
}

// IsTaskIDDuplicate checks the error is a TaskIDDuplicate error or not.
func IsTaskIDDuplicate(err error) bool {
	return errors.Cause(err) == ErrTaskIdDuplicate
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

func IsPieceMd5NotMatch(err error) bool {
	return errors.Cause(err) == ErrPieceMd5NotMatch
}

func IsDataNotFound(err error) bool {
	return errors.Cause(err) == ErrDataNotFound
}

func IsInvalidValue(err error) bool {
	return errors.Cause(err) == ErrInvalidValue
}

func IsConvertFailed(err error) bool {
	return errors.Cause(err) == ErrConvertFailed
}

func IsRangeNotSatisfiable(err error) bool {
	return errors.Cause(err) == ErrRangeNotSatisfiable
}

func IsFileNotExist(err error) bool {
	return errors.Cause(err) == ErrFileNotExist
}
