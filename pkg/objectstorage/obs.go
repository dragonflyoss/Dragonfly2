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

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	huaweiObs "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type obs struct {
	// OBS client.
	client *huaweiObs.ObsClient
}

// New oss instance.
func newOBS(region, endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	client, err := huaweiObs.New(accessKey, secretKey, endpoint)
	if err != nil {
		return nil, fmt.Errorf("new obs client failed: %s", err)
	}

	return &obs{
		client: client,
	}, nil
}

// GetBucketMetadata returns metadata of bucket.
func (o *obs) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	_, err := o.client.GetBucketMetadata(&huaweiObs.GetBucketMetadataInput{Bucket: bucketName})
	if err != nil {
		return nil, err
	}

	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (o *obs) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := o.client.CreateBucket(&huaweiObs.CreateBucketInput{Bucket: bucketName})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (o *obs) DeleteBucket(ctx context.Context, bucketName string) error {
	_, err := o.client.DeleteBucket(bucketName)
	return err
}

// ListBucketMetadatas list bucket meta data of object storage.
func (o *obs) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := o.client.ListBuckets(&huaweiObs.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var metadatas []*BucketMetadata
	for _, bucket := range resp.Buckets {
		metadatas = append(metadatas, &BucketMetadata{
			Name:     bucket.Name,
			CreateAt: bucket.CreationDate,
		})
	}

	return metadatas, nil
}

// GetObjectMetadata returns metadata of object.
func (o *obs) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {
	header, err := o.client.GetObjectMetadata(&huaweiObs.GetObjectMetadataInput{Bucket: bucketName, Key: objectKey})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil, false, nil
		}

		return nil, false, err
	}

	input := &huaweiObs.GetObjectInput{}
	input.Bucket = bucketName
	input.Key = objectKey
	object, err := o.client.GetObject(input)
	if err != nil {
		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: object.ContentDisposition,
		ContentEncoding:    object.ContentEncoding,
		ContentLanguage:    object.ContentLanguage,
		ContentLength:      header.ContentLength,
		ContentType:        header.ContentType,
		ETag:               header.ETag,
		Digest:             header.Metadata[MetaDigest],
	}, true, nil
}

// GetOject returns data of object.
func (o *obs) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	input := &huaweiObs.GetObjectInput{}
	input.Bucket = bucketName
	input.Key = objectKey
	object, err := o.client.GetObject(input)
	if err != nil {
		return nil, err
	}

	return object.Body, nil
}

// PutObject puts data of object.
func (o *obs) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	input := &huaweiObs.PutObjectInput{}
	input.Bucket = bucketName
	input.Key = objectKey
	input.Body = reader

	meta := map[string]string{}
	meta[MetaDigest] = digest
	input.Metadata = meta
	_, err := o.client.PutObject(input)
	if err != nil {
		return err
	}

	return nil
}

// DeleteObject deletes data of object.
func (o *obs) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	_, err := o.client.DeleteObject(&huaweiObs.DeleteObjectInput{Bucket: bucketName, Key: objectKey})
	if err != nil {
		return err
	}

	return nil
}

// ListObjectMetadatas returns metadata of objects.
func (o *obs) ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error) {
	input := &huaweiObs.ListObjectsInput{}
	input.Bucket = bucketName
	input.Marker = marker
	input.Prefix = prefix
	input.MaxKeys = int(limit)
	objects, err := o.client.ListObjects(input)
	if err != nil {
		return nil, err
	}

	var metadatas []*ObjectMetadata
	for _, object := range objects.Contents {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:  object.Key,
			ETag: object.ETag,
		})
	}

	return metadatas, nil
}

// IsObjectExist returns whether the object exists.
func (o *obs) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	_, isExist, err := o.GetObjectMetadata(ctx, bucketName, objectKey)
	if err != nil {
		return false, err
	}

	if !isExist {
		return false, nil
	}

	return true, nil
}

// IsBucketExist returns whether the bucket exists.
func (o *obs) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	_, err := o.client.HeadBucket(bucketName)

	return err == nil, err
}

// GetSignURL returns sign url of object.
func (o *obs) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	var obsHTTPMethod huaweiObs.HttpMethodType
	switch method {
	case MethodGet:
		obsHTTPMethod = huaweiObs.HttpMethodGet
	case MethodPut:
		obsHTTPMethod = huaweiObs.HttpMethodPut
	case MethodHead:
		obsHTTPMethod = huaweiObs.HTTP_HEAD
	case MethodPost:
		obsHTTPMethod = huaweiObs.HttpMethodPost
	case MethodDelete:
		obsHTTPMethod = huaweiObs.HTTP_DELETE
	default:
		return "", fmt.Errorf("not support method %s", method)
	}

	input := &huaweiObs.CreateSignedUrlInput{}
	input.Bucket = bucketName
	input.Key = objectKey
	input.Method = obsHTTPMethod
	input.Expires = int(expire.Milliseconds() / 1000)
	obsURL, err := o.client.CreateSignedUrl(input)
	if err != nil {
		return "", err
	}

	return obsURL.SignedUrl, nil
}
