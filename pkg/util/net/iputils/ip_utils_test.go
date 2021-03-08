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

package iputils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsIPv4(t *testing.T) {
	assert.True(t, IsIPv4("30.225.24.222"))
	assert.False(t, IsIPv4("30.225.24.2222"))
}

func TestExternalIPv4(t *testing.T) {
	ip, err := ExternalIPv4()
	assert.Nil(t, err)

	fmt.Println(ip)

	assert.NotEmpty(t, ip)
}
