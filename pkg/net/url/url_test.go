/*
 *     Copyright 2022 The Dragonfly Authors
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

package url

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterQuery(t *testing.T) {
	url, err := FilterQueryParams("http://www.xx.yy/path?u=f&x=y&m=z&x=s#size", []string{"x", "m"})
	assert.Nil(t, err)
	assert.Equal(t, "http://www.xx.yy/path?u=f#size", url)

	url, err = FilterQueryParams("http://www.xx.yy/path?u=f&x=y&m=z&x=s#size", []string{})
	assert.Nil(t, err)
	assert.Equal(t, "http://www.xx.yy/path?u=f&x=y&m=z&x=s#size", url)

	url, err = FilterQueryParams(":error_url", []string{"x", "m"})
	assert.NotNil(t, err)
	assert.Equal(t, "", url)
}

func TestIsValid(t *testing.T) {
	assert.True(t, IsValid("http://www.x.yy"))
	assert.True(t, IsValid("http://www.x.yy/path"))
	assert.False(t, IsValid("http:///path"))
}
