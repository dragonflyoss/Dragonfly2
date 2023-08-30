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

package objectstorage

import "time"

const (
	// ServiceNameS3 is name of s3 storage.
	ServiceNameS3 = "s3"

	// ServiceNameOSS is name of oss storage.
	ServiceNameOSS = "oss"

	// ServiceNameOBS is name of obs storage.
	ServiceNameOBS = "obs"
)

const (
	// MetaDigest is key of digest meta.
	MetaDigest = "digest"
)

// Method is the client operation method .
type Method string

const (
	// MethodHead is the head operation.
	MethodHead Method = "HEAD"

	// MethodGet is the get operation.
	MethodGet Method = "GET"

	// MethodPut is the put operation.
	MethodPut Method = "PUT"

	// MethodPost is the post operation.
	MethodPost Method = "POST"

	// MethodDelete is the delete operation.
	MethodDelete Method = "Delete"

	// MethodList is the list operation.
	MethodList Method = "List"
)

const (
	// DefaultS3ForcePathStyle is the default force path style of s3.
	DefaultS3ForcePathStyle = true
)

const (
	// OBSStorageClassStandardIA is the standard ia storage class of obs.
	OBSStorageClassStandardIA = "STANDARD_IA"
)

const (
	// DefaultTLSHandshakeTimeout is the default timeout of tls handshake of http client.
	DefaultTLSHandshakeTimeout = 30 * time.Second

	// DefaultResponseHeaderTimeout is the default timeout of response header of http client.
	DefaultResponseHeaderTimeout = 30 * time.Second

	// DefaultIdleConnTimeout is the default timeout of idle connection of http client.
	DefaultIdleConnTimeout = 5 * time.Minute

	// DefaultMaxIdleConnsPerHost is the default max idle connections per host of http client.
	DefaultMaxIdleConnsPerHost = 500

	// DefaultReadBufferSize is the default read buffer size of http client.
	DefaultReadBufferSize = 32 << 10

	// DefaultWriteBufferSize is the default write buffer size of http client.
	DefaultWriteBufferSize = 32 << 10

	// DefaultDisableCompression is the default disable compression of http client.
	DefaultDisableCompression = true

	// DefaultTimeout is the default timeout of http client.
	DefaultTimeout = time.Hour
)
