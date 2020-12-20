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

package base

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type fakeRes struct {
	State *ResponseState
	data  interface{}
}

func TestNewResWithCodeAndMsg(t *testing.T) {
	as := assert.New(t)

	var statPtr *fakeRes
	res := NewResWithCodeAndMsg(statPtr, Code_SUCCESS, "success")

	as.Equal(Code_SUCCESS, res.(*fakeRes).State.Code)
	as.Equal("success", res.(*fakeRes).State.Msg)

	as.Panics(func() {
		NewResWithCodeAndMsg(fakeRes{}, Code_SUCCESS, "success")
	})

	as.Panics(func() {
		NewResWithCodeAndMsg(nil, Code_SUCCESS, "success")
	})
}
