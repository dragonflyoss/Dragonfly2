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

package source

import (
	"io"
	"net/http"
)

type Response struct {
	Status        string
	StatusCode    int
	Header        Header
	Body          io.ReadCloser
	ContentLength int64
}

func NewResponse(rc io.ReadCloser, opts ...func(*Response)) *Response {
	resp := &Response{
		Header:        make(Header),
		Status:        "OK",
		StatusCode:    http.StatusOK,
		Body:          rc,
		ContentLength: -1,
	}

	for _, opt := range opts {
		opt(resp)
	}
	return resp
}

func WithStatus(code int, status string) func(*Response) {
	return func(resp *Response) {
		resp.StatusCode = code
		resp.Status = status
	}
}

func WithContentLength(length int64) func(*Response) {
	return func(resp *Response) {
		resp.ContentLength = length
	}
}

func WithHeader(header map[string]string) func(*Response) {
	return func(resp *Response) {
		for k, v := range header {
			resp.Header[k] = append(resp.Header[k], v)
		}
	}
}

func WithExpireInfo(info ExpireInfo) func(*Response) {
	return func(resp *Response) {
		resp.Header[LastModified] = []string{info.LastModified}
		resp.Header[ETag] = []string{info.ETag}
	}
}
