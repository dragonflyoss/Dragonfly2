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

package errors

import (
	"fmt"

	"github.com/pkg/errors"
)

// ErrURLNotReachable represents the url is a not reachable.
type ErrURLNotReachable struct {
	URL string
}

func (e ErrURLNotReachable) Error() string {
	return fmt.Sprintf("url %s not reachable", e.URL)
}

// ErrTaskIDDuplicate represents the task id is in conflict.
type ErrTaskIDDuplicate struct {
	TaskID string
	Cause  error
}

func (e ErrTaskIDDuplicate) Error() string {
	return fmt.Sprintf("taskId %s conflict: %v", e.TaskID, e.Cause)
}

type ErrInconsistentValues struct {
	Expected interface{}
	Actual   interface{}
}

func (e ErrInconsistentValues) Error() string {
	return fmt.Sprintf("inconsistent number of pieces, expected %s, actual: %s", e.Expected, e.Actual)
}

// ErrResourceExpired represents the downloaded resource has expired
type ErrResourceExpired struct {
	URL string
}

func (e ErrResourceExpired) Error() string {
	return fmt.Sprintf("url %s expired", e.URL)
}

// ErrResourceNotSupportRangeRequest represents the downloaded resource does not support Range downloads
type ErrResourceNotSupportRangeRequest struct {
	URL string
}

func (e ErrResourceNotSupportRangeRequest) Error() string {
	return fmt.Sprintf("url %s does not support range request", e.URL)
}

// ErrFileNotExist represents the file is not exists
type ErrFileNotExist struct {
	File string
}

func (e ErrFileNotExist) Error() string {
	return fmt.Sprintf("file or dir %s not exist", e.File)
}

var (
	// ErrSystemError represents the error is a system error.
	ErrSystemError = errors.New("system error")

	// ErrTaskDownloadFail represents an exception was encountered while downloading the file
	ErrTaskDownloadFail = errors.New("resource download failed")

	// ErrDataNotFound represents the data cannot be found.
	ErrDataNotFound = errors.New("data not found")

	// ErrInvalidValue represents the value is invalid.
	ErrInvalidValue = errors.New("invalid value")

	// ErrConvertFailed represents failed to convert.
	ErrConvertFailed = errors.New("convert failed")

	// ErrResourcesLacked represents a lack of resources, for example, the disk does not have enough space.
	ErrResourcesLacked = errors.New("resources lacked")
)

// IsSystemError checks the error is a system error or not.
func IsSystemError(err error) bool {
	return errors.Cause(err) == ErrSystemError
}

// IsURLNotReachable checks the error is a url not reachable or not.
func IsURLNotReachable(err error) bool {
	err = errors.Cause(err)
	_, ok := err.(ErrURLNotReachable)
	return ok
}

// IsTaskIDDuplicate checks the error is a TaskIDDuplicate error or not.
func IsTaskIDDuplicate(err error) bool {
	err = errors.Cause(err)
	_, ok := err.(ErrTaskIDDuplicate)
	return ok
}

func IsInconsistentValues(err error) bool {
	err = errors.Cause(err)
	_, ok := err.(ErrInconsistentValues)
	return ok
}

func IsDownloadFail(err error) bool {
	return errors.Cause(err) == ErrTaskDownloadFail
}

func IsResourceExpired(err error) bool {
	err = errors.Cause(err)
	_, ok := err.(ErrResourceExpired)
	return ok
}

func IsResourceNotSupportRangeRequest(err error) bool {
	err = errors.Cause(err)
	_, ok := err.(ErrResourceNotSupportRangeRequest)
	return ok
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

func IsFileNotExist(err error) bool {
	err = errors.Cause(err)
	_, ok := err.(ErrFileNotExist)
	return ok
}

func IsResourcesLacked(err error) bool {
	return errors.Cause(err) == ErrResourcesLacked
}
