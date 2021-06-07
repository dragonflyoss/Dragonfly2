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

package main

import (
	"bytes"
	"io"
	"io/ioutil"
)

var data = "hello world"

type client struct {
}

func (c *client) GetContentLength(url string, headers map[string]string) (int64, error) {
	return int64(len(data)), nil
}

func (c *client) Download(url string, headers map[string]string) (io.ReadCloser, map[string]string, error) {
	return ioutil.NopCloser(bytes.NewBufferString(data)), map[string]string{}, nil
}

func DragonflyPluginInit(option map[string]string) (interface{}, map[string]string, error) {
	return &client{}, map[string]string{"type": "test", "name": "df"}, nil
}
