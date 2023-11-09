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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	aliyunoss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-http-utils/headers"
)

type oss struct {
	// OSS client.
	client *aliyunoss.Client

	// region is storage region.
	region string

	// endpoint is datacenter endpoint.
	endpoint string
}

// New oss instance.
func newOSS(region, endpoint, accessKey, secretKey string, httpClient *http.Client) (ObjectStorage, error) {
	client, err := aliyunoss.New(endpoint, accessKey, secretKey, aliyunoss.Region(region), aliyunoss.HTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("new oss client failed: %s", err)
	}

	return &oss{client, region, endpoint}, nil
}

// GetMetadata returns metadata of object storage.
func (o *oss) GetMetadata(ctx context.Context) *Metadata {
	return &Metadata{
		Name:     ServiceNameOSS,
		Region:   o.region,
		Endpoint: o.endpoint,
	}
}

// GetBucketMetadata returns metadata of bucket.
func (o *oss) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	resp, err := o.client.GetBucketInfo(bucketName)
	if err != nil {
		return nil, err
	}

	return &BucketMetadata{
		Name:     resp.BucketInfo.Name,
		CreateAt: resp.BucketInfo.CreationDate,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (o *oss) CreateBucket(ctx context.Context, bucketName string) error {
	return o.client.CreateBucket(bucketName)
}

// DeleteBucket deletes bucket of object storage.
func (o *oss) DeleteBucket(ctx context.Context, bucketName string) error {
	return o.client.DeleteBucket(bucketName)
}

// DeleteBucket deletes bucket of object storage.
func (o *oss) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := o.client.ListBuckets()
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
func (o *oss) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return nil, false, err
	}

	header, err := bucket.GetObjectDetailedMeta(objectKey)
	if err != nil {
		var serr aliyunoss.ServiceError
		if errors.As(err, &serr) && serr.StatusCode == http.StatusNotFound {
			return nil, false, nil
		}

		return nil, false, err
	}

	contentLength, err := strconv.ParseInt(header.Get(headers.ContentLength), 10, 64)
	if err != nil {
		return nil, false, err
	}

	lastModifiedTime, err := time.Parse(http.TimeFormat, header.Get(aliyunoss.HTTPHeaderLastModified))
	if err != nil {
		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: header.Get(headers.ContentDisposition),
		ContentEncoding:    header.Get(headers.ContentEncoding),
		ContentLanguage:    header.Get(headers.ContentLanguage),
		ContentLength:      contentLength,
		ContentType:        header.Get(headers.ContentType),
		ETag:               header.Get(headers.ETag),
		Digest:             header.Get(aliyunoss.HTTPHeaderOssMetaPrefix + MetaDigest),
		LastModifiedTime:   lastModifiedTime,
		StorageClass:       o.getStorageClass(header.Get(aliyunoss.HTTPHeaderOssStorageClass)),
	}, true, nil
}

// GetObjectMetadatas returns the metadatas of the objects.
func (o *oss) GetObjectMetadatas(ctx context.Context, bucketName, prefix, marker, delimiter string, limit int64) (*ObjectMetadatas, error) {
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	resp, err := bucket.ListObjects(aliyunoss.Prefix(prefix), aliyunoss.Marker(marker), aliyunoss.Delimiter(delimiter), aliyunoss.MaxKeys(int(limit)))
	if err != nil {
		return nil, err
	}

	metadatas := make([]*ObjectMetadata, 0, len(resp.Objects))
	for _, object := range resp.Objects {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:              object.Key,
			ETag:             object.ETag,
			ContentLength:    object.Size,
			LastModifiedTime: object.LastModified,
			StorageClass:     o.getStorageClass(object.StorageClass),
		})
	}

	return &ObjectMetadatas{
		Metadatas:      metadatas,
		CommonPrefixes: resp.CommonPrefixes,
	}, nil
}

// GetOject returns data of object.
func (o *oss) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}

	return bucket.GetObject(objectKey)
}

// PutObject puts data of object.
func (o *oss) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return err
	}

	meta := aliyunoss.Meta(MetaDigest, digest)
	return bucket.PutObject(objectKey, reader, meta)
}

// DeleteObject deletes data of object.
func (o *oss) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return err
	}

	return bucket.DeleteObject(objectKey)
}

// IsObjectExist returns whether the object exists.
func (o *oss) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return false, err
	}

	return bucket.IsObjectExist(objectKey)
}

// IsBucketExist returns whether the bucket exists.
func (o *oss) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	return o.client.IsBucketExist(bucketName)
}

// CopyObject copy object from source to destination.
func (o *oss) CopyObject(ctx context.Context, bucketName, sourceObjectKey, destinationObjectKey string) error {
	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return err
	}

	_, err = bucket.CopyObject(sourceObjectKey, destinationObjectKey)
	return err
}

// GetSignURL returns sign url of object.
func (o *oss) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	var ossHTTPMethod aliyunoss.HTTPMethod
	switch method {
	case MethodGet:
		ossHTTPMethod = aliyunoss.HTTPGet
	case MethodPut:
		ossHTTPMethod = aliyunoss.HTTPPut
	case MethodHead:
		ossHTTPMethod = aliyunoss.HTTPHead
	case MethodPost:
		ossHTTPMethod = aliyunoss.HTTPPost
	case MethodDelete:
		ossHTTPMethod = aliyunoss.HTTPDelete
	case MethodList:
		ossHTTPMethod = aliyunoss.HTTPGet
	default:
		return "", fmt.Errorf("not support method %s", method)
	}

	bucket, err := o.client.Bucket(bucketName)
	if err != nil {
		return "", err
	}

	return bucket.SignURL(objectKey, ossHTTPMethod, int64(expire.Seconds()))
}

// getStorageClass returns the default storage class if the input is empty.
func (o *oss) getStorageClass(storageClass string) string {
	if storageClass == "" {
		storageClass = string(aliyunoss.StorageStandard)
	}

	return storageClass
}
