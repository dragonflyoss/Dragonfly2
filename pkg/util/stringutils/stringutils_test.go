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

package stringutils

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

func Test(t *testing.T) {
	suite.Run(t, new(StringUtilSuite))
}

type StringUtilSuite struct {
	suite.Suite
}

func (suite *StringUtilSuite) TestSubString() {
	var cases = []struct {
		str      string
		start    int
		end      int
		expected string
	}{
		{"abcdef", 0, 3, "abc"},
		{"abcdef", 0, 6, "abcdef"},
		{"abcdef", -1, 3, ""},
		{"abcdef", 0, 7, ""},
		{"abcdef", 3, 1, ""},
	}

	for _, v := range cases {
		suite.Equal(SubString(v.str, v.start, v.end), v.expected)
	}
}

func (suite *StringUtilSuite) TestIsEmptyStr() {
	suite.Equal(IsEmptyStr(""),true)
	suite.Equal(IsEmptyStr("  "), true)
	suite.Equal(IsEmptyStr("\n  "), true)
	suite.Equal(IsEmptyStr("x"), false)
}
