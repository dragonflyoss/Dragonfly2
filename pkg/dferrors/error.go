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

import (
	"fmt"
	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/base"
	"github.com/pkg/errors"
)

// errors used only on local
var (
	ErrEndOfStream     = errors.New("end of stream")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrDataNotFound    = errors.New("data not found")
	ErrEmptyValue      = errors.New("empty value")
	ErrConvertFailed   = errors.New("convert failed")
)

func IsEndOfStream(err error) bool {
	return err == ErrEndOfStream
}

type DfError struct {
	Code    base.Code
	Message string
}

func New(code base.Code, msg string) *DfError {
	return &DfError{
		Code:    code,
		Message: msg,
	}
}

func Newf(code base.Code, format string, a ...interface{}) *DfError {
	return &DfError{
		Code:    code,
		Message: fmt.Sprintf(format, a...),
	}
}

func (s *DfError) Error() string {
	return fmt.Sprintf("[%d]%s", s.Code, s.Message)
}

func CheckError(err error, code base.Code) bool {
	if err == nil {
		return false
	}

	e, ok := err.(*DfError)

	return ok && e.Code == code
}
