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

package common

import (
	"reflect"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	commonv1 "d7y.io/api/pkg/apis/common/v1"
)

var EndOfPiece = int32(1) << 30
var BeginOfPiece = int32(-1)

func NewGrpcDfError(code commonv1.Code, msg string) *commonv1.GrpcDfError {
	return &commonv1.GrpcDfError{
		Code:    code,
		Message: msg,
	}
}

// NewResWithCodeAndMsg returns a response ptr with code and msg,
// ptr is a expected type ptr.
func NewResWithCodeAndMsg(ptr any, code commonv1.Code, msg string) any {
	typ := reflect.TypeOf(ptr)
	v := reflect.New(typ.Elem())

	return v.Interface()
}

func NewResWithErr(ptr any, err error) any {
	st := status.Convert(err)
	var code commonv1.Code
	switch st.Code() {
	case codes.DeadlineExceeded:
		code = commonv1.Code_RequestTimeOut
	case codes.OK:
		code = commonv1.Code_Success
	default:
		code = commonv1.Code_UnknownError
	}
	return NewResWithCodeAndMsg(ptr, code, st.Message())
}
