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

package ratelimiter

import (
	"encoding/json"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestSuite(t *testing.T) {
	suite.Run(t, new(RateSuite))
}

type RateSuite struct{
	suite.Suite
}

func (suite *RateSuite) TestParseRate() {
	var cases = []struct {
		input    string
		expected Rate
		isWrong  bool
	}{
		{"5m", 5 * MB, false},
		{"5M", 5 * MB, false},
		{"5MB", 5 * MB, false},
		{"1B", B, false},
		{"100", 100 * B, false},
		{"10K", 10 * KB, false},
		{"10KB", 10 * KB, false},
		{"10k", 10 * KB, false},
		{"10G", 10 * GB, false},
		{"10GB", 10 * GB, false},
		{"10g", 10 * GB, false},
		{"10xx", 0, true},
	}

	for _, cc := range cases {
		output, err := ParseRate(cc.input)
		if !cc.isWrong {
			suite.Nil(err)
			suite.Equal(output, cc.expected)
		} else {
			suite.NotNil(err)
		}

	}
}

func (suite *RateSuite) TestString() {
	var cases = []struct {
		expected string
		input    Rate
	}{
		{"5MB", 5 * MB},
		{"1B", B},
		{"0B", Rate(0)},
		{"10KB", 10 * KB},
		{"1GB", GB},
	}

	for _, cc := range cases {
		suite.Equal(cc.expected, cc.input.String())
	}
}

func (suite *RateSuite) TestMarshalJSON() {
	var cases = []struct {
		input  Rate
		output string
	}{
		{
			5 * MB,
			"\"5MB\"",
		},
		{
			1 * GB,
			"\"1GB\"",
		},
		{
			1 * B,
			"\"1B\"",
		},
		{
			1 * KB,
			"\"1KB\"",
		},
	}

	for _, cc := range cases {
		data, err := json.Marshal(cc.input)
		suite.Nil(err)
		suite.Equal(string(data), cc.output)
	}
}

func (suite *RateSuite) TestUnMarshalJSON() {
	var cases = []struct {
		output Rate
		input  string
	}{
		{
			5 * MB,
			"\"5M\"",
		},
		{
			5 * MB,
			"\"5MB\"",
		},
		{
			5 * MB,
			"\"5m\"",
		},
		{
			1 * GB,
			"\"1GB\"",
		},
		{
			1 * GB,
			"\"1G\"",
		},
		{
			1 * GB,
			"\"1g\"",
		},
		{
			1 * B,
			"\"1B\"",
		},
		{
			1 * B,
			"\"1\"",
		},
		{
			1 * KB,
			"\"1KB\"",
		},
		{
			1 * KB,
			"\"1K\"",
		},
		{
			1 * KB,
			"\"1k\"",
		},
	}

	for _, cc := range cases {
		var r Rate
		err := json.Unmarshal([]byte(cc.input), &r)
		suite.Nil(err)
		suite.Equal(r, cc.output)
	}
}

func (suite *RateSuite) TestMarshalYAML() {
	var cases = []struct {
		input  Rate
		output string
	}{
		{
			5 * MB,
			"5MB\n",
		},
		{
			1 * GB,
			"1GB\n",
		},
		{
			1 * B,
			"1B\n",
		},
		{
			1 * KB,
			"1KB\n",
		},
	}

	for _, cc := range cases {
		data, err := yaml.Marshal(cc.input)
		suite.Nil(err)
		suite.Equal(string(data), cc.output)
	}
}

func (suite *RateSuite) TestUnMarshalYAML() {
	var cases = []struct {
		output Rate
		input  string
	}{
		{
			5 * MB,
			"5M\n",
		},
		{
			1 * GB,
			"1G\n",
		},
		{
			1 * B,
			"1B\n",
		},
		{
			1 * KB,
			"1K\n",
		},
	}

	for _, cc := range cases {
		var output Rate
		err := yaml.Unmarshal([]byte(cc.input), &output)
		suite.Nil(err)
		suite.Equal(output, cc.output)
	}
}
