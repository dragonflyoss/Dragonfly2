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

package types

import "d7y.io/dragonfly/v2/internal/rpc/base"

type PeerRegisterResponse struct {
	// task id
	TaskId string `protobuf:"bytes,2,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	// file content length scope for the url
	SizeScope base.SizeScope `protobuf:"varint,3,opt,name=size_scope,json=sizeScope,proto3,enum=base.SizeScope" json:"size_scope,omitempty"`
	// download the only piece directly for small or tiny file
	//
	// Types that are assignable to DirectPiece:
	//	*RegisterResult_SinglePiece
	//	*RegisterResult_PieceContent
	DirectPiece isRegisterResult_DirectPiece `protobuf_oneof:"direct_piece"`
}
