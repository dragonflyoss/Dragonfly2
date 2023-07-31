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

import "mime/multipart"

const (
	// CopyOperation is the operation of copying object.
	CopyOperation = "copy"
)

type BucketParams struct {
	// ID is the id of the bucket.
	ID string `uri:"id" binding:"required"`
}

type ObjectParams struct {
	// ID is the id of the bucket.
	ID string `uri:"id" binding:"required"`

	// ObjectKey is the object key.
	ObjectKey string `uri:"object_key" binding:"required"`
}

type PutObjectRequest struct {
	// Mode is the mode of putting object.
	Mode uint `form:"mode,default=0" binding:"omitempty,gte=0,lte=2"`

	// Filter is the filter of the object.
	Filter string `form:"filter" binding:"omitempty"`

	// MaxReplicas is the max replicas of the object.
	MaxReplicas int `form:"maxReplicas" binding:"omitempty,gt=0,lte=100"`

	// File is the file of the object.
	File *multipart.FileHeader `form:"file" binding:"required"`
}

type GetObjectQuery struct {
	// Filter is the filter of the object.
	Filter string `form:"filter" binding:"omitempty"`
}

type GetObjectMetadatasQuery struct {
	// A delimiter is a character used to group keys.
	Delimiter string `form:"delimiter" binding:"omitempty"`

	// Marker indicates the starting object key for listing.
	Marker string `form:"marker" binding:"omitempty"`

	// Sets the maximum number of keys returned in the response.
	Limit int64 `form:"limit" binding:"omitempty"`

	// Limits the response to keys that begin with the specified prefix.
	Prefix string `form:"prefix" binding:"omitempty"`
}

type CopyObjectRequest struct {
	// SourceBucket is the source object key.
	SourceObjectKey string `form:"source_object_key" binding:"required"`
}
