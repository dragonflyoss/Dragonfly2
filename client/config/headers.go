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

const (
	HeaderDragonflyFilter = "X-Dragonfly-Filter"
	HeaderDragonflyPeer   = "X-Dragonfly-Peer"
	HeaderDragonflyTask   = "X-Dragonfly-Task"
	HeaderDragonflyRange  = "X-Dragonfly-Range"
	// HeaderDragonflyTag different HeaderDragonflyTag for the same url will be divided into different P2P overlay
	HeaderDragonflyTag = "X-Dragonfly-Tag"
	// HeaderDragonflyApplication is used for statistics and traffic control
	HeaderDragonflyApplication = "X-Dragonfly-Application"
	// HeaderDragonflyPriority scheduler will schedule tasks according to priority
	HeaderDragonflyPriority = "X-Dragonfly-Priority"
	// HeaderDragonflyRegistry is used for dynamic registry mirrors.
	HeaderDragonflyRegistry = "X-Dragonfly-Registry"
	// HeaderDragonflyObjectMetaDigest is used for digest of object storage.
	HeaderDragonflyObjectMetaDigest = "X-Dragonfly-Object-Meta-Digest"
	// HeaderDragonflyObjectMetaLastModifiedTime is used for last modified time of object storage.
	HeaderDragonflyObjectMetaLastModifiedTime = "X-Dragonfly-Object-Meta-Last-Modified-Time"
	// HeaderDragonflyObjectMetaStorageClass is used for storage class of object storage.
	HeaderDragonflyObjectMetaStorageClass = "X-Dragonfly-Object-Meta-Storage-Class"
	// HeaderDragonflyObjectOperation is used for object storage operation.
	HeaderDragonflyObjectOperation = "X-Dragonfly-Object-Operation"
)
