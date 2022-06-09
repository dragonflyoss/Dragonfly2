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

package service

import (
	"context"
	"errors"

	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/objectstorage"
)

var ErrObjectStorageDisabled = errors.New("object storage is disabled")

func (s *service) CreateBucket(ctx context.Context, json types.CreateBucketRequest) error {
	if s.objectStorage == nil {
		return ErrObjectStorageDisabled
	}

	return s.objectStorage.CreateBucket(ctx, json.Name)
}

func (s *service) DestroyBucket(ctx context.Context, id string) error {
	if s.objectStorage == nil {
		return ErrObjectStorageDisabled
	}

	return s.objectStorage.DeleteBucket(ctx, id)
}

func (s *service) GetBucket(ctx context.Context, id string) (*objectstorage.BucketMetadata, error) {
	if s.objectStorage == nil {
		return nil, ErrObjectStorageDisabled
	}

	return s.objectStorage.GetBucketMetadata(ctx, id)
}

func (s *service) GetBuckets(ctx context.Context) ([]*objectstorage.BucketMetadata, error) {
	if s.objectStorage == nil {
		return nil, ErrObjectStorageDisabled
	}

	return s.objectStorage.ListBucketMetadatas(ctx)
}
