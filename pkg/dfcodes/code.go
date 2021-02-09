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

package dfcodes

import "d7y.io/dragonfly/v2/pkg/rpc/base"

// rpc response code
const (
	// no problem 200-299
	Success base.Code = 200

	ErrInvalidValue
	// client processing error 400-499
	BadRequest base.Code = 400

	// scheduler processing error 5000-5999
	SchedulerError        base.Code = 5000
	PeerTaskNotRegistered base.Code = 5001
	// client can be migrated to another scheduler
	ResourceLacked base.Code = 5002
	// client should try to download from source
	BackSource base.Code = 5003

	// cdnsystem processing error 6000-6999
	CdnError            base.Code = 6000
	CdnTaskRegistryFail base.Code = 6001
	CdnTaskNotFound     base.Code = 6404

	// manager processing error 7000-7999
	ManagerError base.Code = 7000

	// shared error 1000-1099
	UnknownError    base.Code = 1000
	InvalidArgument base.Code = 1001
	RequestTimeOut  base.Code = 1002
)
