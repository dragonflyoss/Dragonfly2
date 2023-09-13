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

package config

import (
	"os"
	"strings"
	"syscall"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/util"
	"d7y.io/dragonfly/v2/cmd/dependency/base"
)

func TestDfgetConfig_Validate(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *ClientOption
		expect func(t *testing.T, err error)
	}{
		{
			name: "no error",
			cfg: &ClientOption{
				URL:                  "http://path",
				RecursiveAcceptRegex: "(a|b)",
				RecursiveRejectRegex: "(a|b)",
				Output:               "/tmp/df/test",
				Header: []string{
					"Accept: *",
					"Host: abc",
				},
				RateLimit: util.RateLimit{Limit: 20971520},
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.Equal(nil, err)
			},
		},
		{
			name: "runtime config is nil",
			cfg:  nil,
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "runtime config: invalid argument")
			},
		},
		{
			name: "url is invaild",
			cfg: &ClientOption{
				URL: "http:///path",
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "url http:///path: invalid argument")
			},
		},
		{
			name: "recursive accept regex is invaild",
			cfg: &ClientOption{
				URL:                  "http://path",
				RecursiveAcceptRegex: "(a|b))",
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "error parsing regexp: unexpected ): `(a|b))`")
			},
		},
		{
			name: "recursive reject regex is invaild",
			cfg: &ClientOption{
				URL:                  "http://path",
				RecursiveRejectRegex: "(a|b))",
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "error parsing regexp: unexpected ): `(a|b))`")
			},
		},
		{
			name: "output path is not absolute path",
			cfg: &ClientOption{
				URL:    "http://path",
				Output: "tmp/df/test",
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "output path[tmp/df/test] is not absolute path: invalid argument")
			},
		},
		{
			name: "header is invalid",
			cfg: &ClientOption{
				URL:    "http://path",
				Output: "/tmp/df/test",
				Header: []string{
					"Accept: *",
					"Host: ",
				},
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "output header format error: Host: : invalid Header")
			},
		},
		{
			name: "rate limit is invalid",
			cfg: &ClientOption{
				URL:       "http://path",
				Output:    "/tmp/df/test",
				RateLimit: util.RateLimit{Limit: 20971519},
			},
			expect: func(t *testing.T, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "rate limit must be greater than 20.0MB: invalid argument")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			tc.expect(t, err)
		})
	}
}

func TestDfgetConfig_Convert(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *ClientOption
		args   []string
		expect func(t *testing.T, cfg *ClientOption, err error)
	}{
		{
			name: "Output is nil",
			cfg: &ClientOption{
				URL:    "http://path/to/file/",
				Output: "",
			},
			expect: func(t *testing.T, cfg *ClientOption, err error) {
				assert := testifyassert.New(t)
				assert.NoError(err)
			},
		},
		{
			name: "URL is nil",
			cfg: &ClientOption{
				URL:    "",
				Output: "/path/to/file",
				Digest: "11111111111111111111111111111111",
				Tag:    "v1.0.0",
				Options: base.Options{
					Console: true,
				},
				ShowProgress: true,
			},
			args: []string{"http://path/to/file"},
			expect: func(t *testing.T, cfg *ClientOption, err error) {
				assert := testifyassert.New(t)
				assert.NoError(err)
				assert.Equal("", cfg.Tag)
				assert.Equal(false, cfg.ShowProgress)
			},
		},
		{
			name: "URL is invaild",
			cfg: &ClientOption{
				URL:    "file/",
				Output: "",
			},
			expect: func(t *testing.T, cfg *ClientOption, err error) {
				assert := testifyassert.New(t)
				assert.EqualError(err, "get output from url[file/] error")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Convert(tc.args)
			tc.expect(t, tc.cfg, err)
		})
	}
}

func TestMkdirAllRoot(t *testing.T) {
	assert := testifyassert.New(t)
	err := MkdirAll("/", 0700, os.Getuid(), os.Getgid())
	assert.Nil(err, "mkdir should not return error")
}

func TestMkdirAll(t *testing.T) {
	uid := os.Getuid()
	gid := os.Getgid()
	syscall.Umask(0)
	os.RemoveAll("/tmp/df/test")

	tests := []struct {
		name    string
		dir     string
		parent  string
		newDirs []string
		uid     int
		gid     int
	}{
		{
			name:   "dir exists",
			dir:    "/tmp/df/test",
			parent: "/tmp/df/test",
			uid:    uid,
			gid:    gid,
		},
		{
			name:   "dir exists",
			dir:    "/tmp/df/test",
			parent: "/tmp/df/test",
			uid:    uid,
			gid:    gid,
		},
		{
			name:   "new dir with uid:gid, single layer",
			dir:    "/tmp/df/test/x",
			parent: "/tmp/df/test",
			newDirs: []string{
				"/tmp/df/test/x/",
			},
			uid: uid,
			gid: gid,
		},
		{
			name:   "new dir with uid:gid, multi layer",
			dir:    "/tmp/df/test/x/y/z",
			parent: "/tmp/df/test",
			newDirs: []string{
				"/tmp/df/test/x",
				"/tmp/df/test/x/y",
				"/tmp/df/test/x/y/z",
			},
			uid: uid,
			gid: gid,
		},
		{
			name:   "new dir with uid:0, single layer",
			dir:    "/tmp/df/test/x",
			parent: "/tmp/df/test",
			newDirs: []string{
				"/tmp/df/test/x",
			},
			uid: uid,
			gid: 0,
		},
		{
			name:   "new dir with uid:0, multi layer",
			dir:    "/tmp/df/test/x/y/z",
			parent: "/tmp/df/test",
			newDirs: []string{
				"/tmp/df/test/x",
				"/tmp/df/test/x/y",
				"/tmp/df/test/x/y/z",
			},
			uid: uid,
			gid: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)
			// prepare parent
			ok := strings.HasPrefix(tc.parent, "/tmp/df/test")
			assert.True(ok, "should in test dir, avoid operating import directories")
			if !ok {
				return
			}
			assert.Nil(os.MkdirAll(tc.parent, 0700))
			defer func() {
				// remove parent directory
				assert.Nil(os.RemoveAll(tc.parent))
			}()

			err := MkdirAll(tc.dir, 0700, tc.uid, tc.gid)
			assert.Nil(err, "mkdir should not return error")

			// check new directories' permission
			for _, dir := range tc.newDirs {
				info, err := os.Stat(dir)
				assert.Nil(err)
				if err != nil {
					continue
				}
				stat, ok := info.Sys().(*syscall.Stat_t)
				assert.True(ok)
				assert.Equal(tc.uid, int(stat.Uid))
				assert.Equal(tc.gid, int(stat.Gid))
			}
		})
	}
}

func TestCheckHeader(t *testing.T) {
	cfg := NewDfgetConfig()

	// test empty header
	testifyassert.Nil(t, cfg.checkHeader())

	tests := []struct {
		header string
		hasErr bool
	}{
		{
			header: "",
			hasErr: true,
		},
		{
			header: "#",
			hasErr: true,
		},
		{
			header: "a:b",
			hasErr: false,
		},
		{
			header: "1:2",
			hasErr: false,
		},
		{
			header: "a:b:c",
			hasErr: false,
		},
		{
			header: ":",
			hasErr: true,
		},
		{
			header: " :b",
			hasErr: true,
		},
		{
			header: "a: ",
			hasErr: true,
		},
	}

	for _, test := range tests {
		cfg.Header = []string{test.header}
		if test.hasErr {
			testifyassert.NotNil(t, cfg.checkHeader())
		} else {
			testifyassert.Nil(t, cfg.checkHeader())
		}
	}
}
