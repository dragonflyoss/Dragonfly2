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

package unit

import (
	"regexp"
	"testing"

	"github.com/bmizerany/assert"
	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func Test_parseByte(t *testing.T) {
	assert := testifyassert.New(t)
	testCases := []struct {
		data   []string
		size   int64
		failed bool
	}{
		{
			data: []string{
				"1Ka",
				"1Kia",
				"1KBa",
				"1KiBa",
				"1024a",
			},
			failed: true,
		},
		{
			data: []string{
				"1K",
				"1Ki",
				"1KB",
				"1KiB",
				"1024",
			},
			size: 1024,
		},
		{
			data: []string{
				"1024K",
				"1024Ki",
				"1M",
				"1Mi",
			},
			size: int64(MB),
		},
		{
			data: []string{
				"1024M",
				"1024Mi",
				"1G",
				"1Gi",
			},
			size: int64(GB),
		},
		{
			data: []string{
				"1024G",
				"1024Gi",
				"1T",
				"1Ti",
			},
			size: int64(TB),
		},
		{
			data: []string{
				"1024T",
				"1024Ti",
				"1P",
				"1Pi",
			},
			size: int64(PB),
		},
		{
			data: []string{
				"1024P",
				"1024Pi",
				"1E",
				"1Ei",
			},
			size: int64(EB),
		},
	}

	for _, tc := range testCases {
		for _, d := range tc.data {
			b, err := parseSize(d)
			if tc.failed {
				assert.NotNil(err)
			} else {
				assert.Nil(err)
			}
			if err != nil {
				continue
			}
			assert.Equal(tc.size, b.ToNumber())
		}
	}
}

func TestByteUnmarshal(t *testing.T) {
	assert := testifyassert.New(t)
	testCases := []struct {
		data   string
		size   int64
		failed bool
	}{
		{
			data: `
size: 1K
`,
			size: 1024,
		},
		{
			data: `
size: 1Ki
`,
			size: 1024,
		},
		{
			data: `
size: 1Mi
`,
			size: int64(MB),
		},
		{
			data: `
size: 1Mix
`,
			size:   int64(MB),
			failed: true,
		},
	}
	for _, tc := range testCases {
		data := struct {
			Size Bytes `yaml:"size"`
		}{}
		err := yaml.Unmarshal([]byte(tc.data), &data)
		if tc.failed {
			assert.NotNil(err)
		} else {
			assert.Nil(err)
		}
		if err != nil {
			continue
		}
		assert.Equal(tc.size, data.Size.ToNumber())
	}
}

func TestUnmarshal(t *testing.T) {
	p := regexp.MustCompile("/\\S*/")
	assert.Equal(t, p.MatchString("ddd"), false)
	//assert.Equal(t, p.MatchString(""), false)
	//assert.Equal(t, p.MatchString("  "), false)
}
