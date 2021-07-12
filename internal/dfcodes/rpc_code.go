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

import "d7y.io/dragonfly.v2/pkg/rpc/base"

// rpc code
const (
	// success code 200-299
	Success           base.Code = 200
	ServerUnavailable base.Code = 500 // framework can not find server node

	// common response error 1000-1999
	ResourceLacked   base.Code = 1000 // client can be migrated to another scheduler
	BadRequest       base.Code = 1400
	PeerTaskNotFound base.Code = 1404
	UnknownError     base.Code = 1500
	RequestTimeOut   base.Code = 1504

	// client response error 4000-4999
	ClientError             base.Code = 4000
	ClientPieceRequestFail  base.Code = 4001 // get piece task from other peer error
	ClientScheduleTimeout   base.Code = 4002 // wait scheduler response timeout
	ClientContextCanceled   base.Code = 4003
	ClientWaitPieceReady    base.Code = 4004 // when target peer downloads from source slowly, should wait
	ClientPieceDownloadFail base.Code = 4005
	ClientRequestLimitFail  base.Code = 4006

	// scheduler response error 5000-5999
	SchedError          base.Code = 5000
	SchedNeedBackSource base.Code = 5001 // client should try to download from source
	SchedPeerGone       base.Code = 5002 // client should disconnect from scheduler

	// cdnsystem response error 6000-6999
	CdnError            base.Code = 6000
	CdnTaskRegistryFail base.Code = 6001
	CdnTaskDownloadFail base.Code = 6002
	CdnTaskNotFound     base.Code = 6404

	// manager response error 7000-7999
	ManagerError           base.Code = 7000
	InvalidResourceType    base.Code = 7001
	ManagerHostError       base.Code = 7002
	ManagerStoreError      base.Code = 7003
	ManagerConfigError     base.Code = 7004
	ManagerStoreNotFound   base.Code = 7005
	SchedulerNodesNotFound base.Code = 7006
)
