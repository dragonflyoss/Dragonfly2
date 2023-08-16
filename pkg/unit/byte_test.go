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
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func Test_Set(t *testing.T) {
	assert := testifyassert.New(t)
	testCases := []struct {
		sizeString string
		sizeNumber int64
		failed     bool
	}{
		{
			sizeString: "",
			sizeNumber: 0,
		},
		{
			sizeString: "1B",
			sizeNumber: 1,
		},
		{
			sizeString: "2KB",
			sizeNumber: 2 * 1024,
		},
		{
			sizeString: "3MB",
			sizeNumber: 3 * 1024 * 1024,
		},
		{
			sizeString: "4GB",
			sizeNumber: 4 * 1024 * 1024 * 1024,
		},
		{
			sizeString: "5TB",
			sizeNumber: 5 * 1024 * 1024 * 1024 * 1024,
		},
		{
			sizeString: "6PB",
			sizeNumber: 6 * 1024 * 1024 * 1024 * 1024 * 1024,
		},
		{
			sizeString: "7EB",
			sizeNumber: 7 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
		},
		{
			sizeString: "8unknown",
			failed:     true,
		},
	}

	for _, tc := range testCases {
		var b Bytes
		err := b.Set(tc.sizeString)
		if tc.failed {
			assert.NotNil(err)
		} else {
			assert.Nil(err)
		}
		if err != nil {
			continue
		}
		assert.Equal(tc.sizeNumber, b.ToNumber())
	}
}

func Test_Type(t *testing.T) {
	assert := testifyassert.New(t)
	var f Bytes
	assert.Equal("bytes", f.Type())
}

func Test_parseByte(t *testing.T) {
	assert := testifyassert.New(t)
	testCases := []struct {
		data   []string
		size   int64
		failed bool
	}{
		{
			data: []string{
				"1234567890123456789012345678901234567890123456789012345678901234567890",
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
		{
			data: []string{
				"",
			},
			size: 0,
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

func TestByteMarshalYAML(t *testing.T) {
	tests := []struct {
		name   string
		f      Bytes
		expect any
	}{
		{
			name:   "3MB",
			f:      3 * 1024 * 1024,
			expect: "3.0MB",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)
			result, _ := tc.f.MarshalYAML()
			assert.Equal(tc.expect, result)
		})
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

func Test_String(t *testing.T) {
	assert := testifyassert.New(t)
	testCases := []struct {
		data string
		b    Bytes
	}{
		{
			data: "1.0B",
			b:    1,
		},
		{
			data: "2.0KB",
			b:    2 * 1024,
		},
		{
			data: "3.0MB",
			b:    3 * 1024 * 1024,
		},
		{
			data: "4.0GB",
			b:    4 * 1024 * 1024 * 1024,
		},
		{
			data: "5.0TB",
			b:    5 * 1024 * 1024 * 1024 * 1024,
		},
		{
			data: "6.0PB",
			b:    6 * 1024 * 1024 * 1024 * 1024 * 1024,
		},
	}

	for _, tc := range testCases {
		assert.Equal(tc.b.String(), tc.data)
	}
}
