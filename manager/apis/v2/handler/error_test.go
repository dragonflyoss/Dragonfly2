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

package handler

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/dferrors"

	"github.com/stretchr/testify/assert"
)

func TestNewError(t *testing.T) {

	err := NewError(http.StatusBadRequest, errors.New("test error"))
	httpErr, ok := err.(*HTTPError)
	assert.Equal(t, ok, true, "it shoud be true.")
	assert.Equal(t, httpErr.Code, http.StatusBadRequest, fmt.Sprintf("it shoud be %d", http.StatusBadRequest))
	assert.Equal(t, httpErr.Error(), err.Error(), fmt.Sprintf("%s", err.Error()))

	err = NewError(-1, errors.New("test error"))
	httpErr, ok = err.(*HTTPError)
	assert.Equal(t, ok, true, "it shoud be true.")
	assert.Equal(t, httpErr.Code, http.StatusNotFound, fmt.Sprintf("it shoud be %d", http.StatusNotFound))
	assert.Equal(t, httpErr.Error(), err.Error(), fmt.Sprintf("%s", err.Error()))

	err = NewError(-1, dferrors.New(dfcodes.InvalidResourceType, "InvalidResourceType"))
	httpErr, ok = err.(*HTTPError)
	assert.Equal(t, ok, true, "it shoud be true.")
	assert.Equal(t, httpErr.Code, http.StatusBadRequest, fmt.Sprintf("it shoud be %d", http.StatusBadRequest))
	assert.Equal(t, httpErr.Error(), err.Error(), fmt.Sprintf("%s", err.Error()))
}
