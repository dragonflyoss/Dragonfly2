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

import "github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"

const (
	// no problem 200-299
	Success base.Code = 200

	// client processing error 400-499
	ClientError base.Code = 400

	// scheduler processing error 500-599
	SchedulerError        base.Code = 500
	PeerTaskNotRegistered base.Code = 501
	// client can be migrated to another scheduler
	ResourceLacked base.Code = 502

	// cdnsystem processing error 600-699
	CdnError base.Code = 600

	// manager processing error 700-799
	ManagerError base.Code = 700

	// shared error 1000-1099
	UnknownError    base.Code = 1000
	InvalidArgument base.Code = 1001
	RequestTimeOut  base.Code = 1002
)
