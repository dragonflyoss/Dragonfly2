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
	// Default number of cdn load limit
	DefaultCDNLoadLimit = 300

	// Default number of client load limit
	DefaultClientLoadLimit = 50

	// Default number of pieces to download in parallel
	DefaultClientParallelCount = 4

	// Default limit the number of filter traversals
	DefaultSchedulerFilterParentLimit = 3
)
