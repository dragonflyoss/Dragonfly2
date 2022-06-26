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
)

func TestMkdirAllRoot(t *testing.T) {
	assert := testifyassert.New(t)
	err := MkdirAll("/", 0777, os.Getuid(), os.Getgid())
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
			assert.Nil(os.MkdirAll(tc.parent, 0777))
			defer func() {
				// remove parent directory
				assert.Nil(os.RemoveAll(tc.parent))
			}()

			err := MkdirAll(tc.dir, 0777, tc.uid, tc.gid)
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
