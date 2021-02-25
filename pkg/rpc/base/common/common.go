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
	"fmt"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"reflect"
)

var EndOfPiece = int32(1) << 30
var ZeroOfPiece = int32(-1)

// cdn peer id suffix
var CdnSuffix = "_CDN"

func NewState(code base.Code, msg interface{}) *base.ResponseState {
	if msg == nil {
		msg = ""
	}

	return &base.ResponseState{
		Code:    code,
		Success: code >= dfcodes.Success && code < (dfcodes.Success+100),
		Msg:     fmt.Sprintf("%v", msg),
	}
}

// NewResWithCodeAndMsg returns a response ptr with code and msg,
// ptr is a expected type ptr.
func NewResWithCodeAndMsg(ptr interface{}, code base.Code, msg string) interface{} {
	typ := reflect.TypeOf(ptr)
	v := reflect.New(typ.Elem())

	state := NewState(code, msg)
	v.Elem().FieldByName("State").Set(reflect.ValueOf(state))

	return v.Interface()
}

func NewResWithErr(ptr interface{}, err error) interface{} {
	st := status.Convert(err)
	var code base.Code
	switch st.Code() {
	case codes.DeadlineExceeded:
		code = dfcodes.RequestTimeOut
	case codes.OK:
		code = dfcodes.Success
	default:
		code = dfcodes.UnknownError
	}
	return NewResWithCodeAndMsg(ptr, code, st.Message())
}
