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

// RequestHeader request header
type RequestHeader map[string]string

func (h RequestHeader) Get(key string) string {
	if h == nil {
		return ""
	}
	return h[key]
}

// ResponseHeader response header
type ResponseHeader map[string]string

func (h ResponseHeader) Get(key string) string {
	if h == nil {
		return ""
	}
	return h[key]
}

const (
	LastModified = "Last-Modified"
	ETag         = "ETag"
)
