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

const (
	// ServiceNameS3 is name of s3 storage.
	ServiceNameS3 = "s3"

	// ServiceNameOSS is name of oss storage.
	ServiceNameOSS = "oss"
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
