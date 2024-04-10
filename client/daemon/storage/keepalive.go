/*
 *     Copyright 2024 The Dragonfly Authors
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

package storage

import (
	"context"
	"io"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	"d7y.io/dragonfly/v2/client/util"
)

type keepAliveTaskStorageDriver struct {
	TaskStorageDriver
	util.KeepAlive
}

func (k *keepAliveTaskStorageDriver) WritePiece(ctx context.Context, req *WritePieceRequest) (int64, error) {
	k.Keep()
	return k.TaskStorageDriver.WritePiece(ctx, req)
}

func (k *keepAliveTaskStorageDriver) ReadPiece(ctx context.Context, req *ReadPieceRequest) (io.Reader, io.Closer, error) {
	k.Keep()
	return k.TaskStorageDriver.ReadPiece(ctx, req)
}

func (k *keepAliveTaskStorageDriver) ReadAllPieces(ctx context.Context, req *ReadAllPiecesRequest) (io.ReadCloser, error) {
	k.Keep()
	return k.TaskStorageDriver.ReadAllPieces(ctx, req)
}

func (k *keepAliveTaskStorageDriver) GetPieces(ctx context.Context, req *commonv1.PieceTaskRequest) (*commonv1.PiecePacket, error) {
	k.Keep()
	return k.TaskStorageDriver.GetPieces(ctx, req)
}

func (k *keepAliveTaskStorageDriver) GetTotalPieces(ctx context.Context, req *PeerTaskMetadata) (int32, error) {
	k.Keep()
	return k.TaskStorageDriver.GetTotalPieces(ctx, req)
}

func (k *keepAliveTaskStorageDriver) GetExtendAttribute(ctx context.Context, req *PeerTaskMetadata) (*commonv1.ExtendAttribute, error) {
	k.Keep()
	return k.TaskStorageDriver.GetExtendAttribute(ctx, req)
}

func (k *keepAliveTaskStorageDriver) UpdateTask(ctx context.Context, req *UpdateTaskRequest) error {
	k.Keep()
	return k.TaskStorageDriver.UpdateTask(ctx, req)
}

func (k *keepAliveTaskStorageDriver) Store(ctx context.Context, req *StoreRequest) error {
	k.Keep()
	return k.TaskStorageDriver.Store(ctx, req)
}

func (k *keepAliveTaskStorageDriver) ValidateDigest(req *PeerTaskMetadata) error {
	k.Keep()
	return k.TaskStorageDriver.ValidateDigest(req)
}
