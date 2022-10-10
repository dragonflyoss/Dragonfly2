/*
 *     Copyright 2022 The Dragonfly Authors
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

type Metadata struct {
	Status     string
	StatusCode int
	Header     Header
	// SupportRange indicates source supports partial download, like Range in http request
	SupportRange bool
	// ContentLength indicates the current content length for the target request
	// ContentLength int64
	// TotalContentLength indicates the total content length for the target request
	// eg, for http response header:
	//      Content-Range: bytes 0-9/2443
	// 2443 is the TotalContentLength, 10 is the ContentLength
	TotalContentLength int64

	Validate  func() error
	Temporary bool
}
