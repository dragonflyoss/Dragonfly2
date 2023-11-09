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
	"net/http"
	"strings"
	"time"

	huaweiobs "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type obs struct {
	// OBS client.
	client *huaweiobs.ObsClient

	// region is storage region.
	region string

	// endpoint is datacenter endpoint.
	endpoint string
}

// New oss instance.
func newOBS(region, endpoint, accessKey, secretKey string, httpClient *http.Client) (ObjectStorage, error) {
	client, err := huaweiobs.New(accessKey, secretKey, endpoint, huaweiobs.WithHttpClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("new obs client failed: %s", err)
	}

	return &obs{client, region, endpoint}, nil
}

// GetMetadata returns metadata of object storage.
func (o *obs) GetMetadata(ctx context.Context) *Metadata {
	return &Metadata{
		Name:     ServiceNameOBS,
		Region:   o.region,
		Endpoint: o.endpoint,
	}
}

// GetBucketMetadata returns metadata of bucket.
func (o *obs) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	if _, err := o.client.GetBucketMetadata(&huaweiobs.GetBucketMetadataInput{Bucket: bucketName}); err != nil {
		return nil, err
	}

	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (o *obs) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := o.client.CreateBucket(&huaweiobs.CreateBucketInput{Bucket: bucketName})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (o *obs) DeleteBucket(ctx context.Context, bucketName string) error {
	_, err := o.client.DeleteBucket(bucketName)
	return err
}

// ListBucketMetadatas list bucket meta data of object storage.
func (o *obs) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := o.client.ListBuckets(&huaweiobs.ListBucketsInput{})
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
	metadata, err := o.client.GetObjectMetadata(&huaweiobs.GetObjectMetadataInput{Bucket: bucketName, Key: objectKey})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil, false, nil
		}

		return nil, false, err
	}

	object, err := o.client.GetObject(&huaweiobs.GetObjectInput{
		GetObjectMetadataInput: huaweiobs.GetObjectMetadataInput{
			Bucket: bucketName,
			Key:    objectKey,
		},
	})
	if err != nil {
		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: object.ContentDisposition,
		ContentEncoding:    object.ContentEncoding,
		ContentLanguage:    object.ContentLanguage,
		ContentLength:      metadata.ContentLength,
		ContentType:        metadata.ContentType,
		ETag:               metadata.ETag,
		Digest:             metadata.Metadata[MetaDigest],
		LastModifiedTime:   metadata.LastModified,
		StorageClass:       o.getStorageClass(metadata.StorageClass),
	}, true, nil
}

// GetObjectMetadatas returns the metadatas of the objects.
func (o *obs) GetObjectMetadatas(ctx context.Context, bucketName, prefix, marker, delimiter string, limit int64) (*ObjectMetadatas, error) {
	resp, err := o.client.ListObjects(&huaweiobs.ListObjectsInput{
		ListObjsInput: huaweiobs.ListObjsInput{
			Prefix:    prefix,
			MaxKeys:   int(limit),
			Delimiter: delimiter,
		},
		Bucket: bucketName,
		Marker: marker,
	})
	if err != nil {
		return nil, err
	}

	metadatas := make([]*ObjectMetadata, 0, len(resp.Contents))
	for _, content := range resp.Contents {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:              content.Key,
			ETag:             content.ETag,
			LastModifiedTime: content.LastModified,
			StorageClass:     o.getStorageClass(content.StorageClass),
		})
	}

	return &ObjectMetadatas{
		Metadatas:      metadatas,
		CommonPrefixes: resp.CommonPrefixes,
	}, nil
}

// GetOject returns data of object.
func (o *obs) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	resp, err := o.client.GetObject(&huaweiobs.GetObjectInput{
		GetObjectMetadataInput: huaweiobs.GetObjectMetadataInput{
			Bucket: bucketName,
			Key:    objectKey,
		},
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// PutObject puts data of object.
func (o *obs) PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	_, err := o.client.PutObject(&huaweiobs.PutObjectInput{
		PutObjectBasicInput: huaweiobs.PutObjectBasicInput{
			ObjectOperationInput: huaweiobs.ObjectOperationInput{
				Bucket: bucketName,
				Key:    objectKey,
				Metadata: map[string]string{
					MetaDigest: digest,
				},
			},
		},
		Body: reader,
	})

	return err
}

// DeleteObject deletes data of object.
func (o *obs) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	_, err := o.client.DeleteObject(&huaweiobs.DeleteObjectInput{Bucket: bucketName, Key: objectKey})
	return err
}

// CopyObject copy object from source to destination.
func (o *obs) CopyObject(ctx context.Context, bucketName, sourceObjectKey, destinationObjectKey string) error {
	_, err := o.client.CopyObject(&huaweiobs.CopyObjectInput{
		ObjectOperationInput: huaweiobs.ObjectOperationInput{
			Bucket: bucketName,
			Key:    destinationObjectKey,
		},
		CopySourceBucket: bucketName,
		CopySourceKey:    sourceObjectKey,
	})

	return err
}

// IsObjectExist returns whether the object exists.
func (o *obs) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	_, isExist, err := o.GetObjectMetadata(ctx, bucketName, objectKey)
	return isExist, err
}

// IsBucketExist returns whether the bucket exists.
func (o *obs) IsBucketExist(ctx context.Context, bucketName string) (bool, error) {
	if _, err := o.client.HeadBucket(bucketName); err != nil {
		return false, err
	}

	return true, nil
}

// GetSignURL returns sign url of object.
func (o *obs) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	var obsHTTPMethod huaweiobs.HttpMethodType
	switch method {
	case MethodGet:
		obsHTTPMethod = huaweiobs.HttpMethodGet
	case MethodPut:
		obsHTTPMethod = huaweiobs.HttpMethodPut
	case MethodHead:
		obsHTTPMethod = huaweiobs.HTTP_HEAD
	case MethodPost:
		obsHTTPMethod = huaweiobs.HttpMethodPost
	case MethodDelete:
		obsHTTPMethod = huaweiobs.HTTP_DELETE
	default:
		return "", fmt.Errorf("not support method %s", method)
	}

	resp, err := o.client.CreateSignedUrl(&huaweiobs.CreateSignedUrlInput{
		Bucket: bucketName,
		Key:    objectKey,
		Method: obsHTTPMethod,
	})
	if err != nil {
		return "", err
	}

	return resp.SignedUrl, nil
}

// getStorageClass returns the default storage class if the input is empty.
func (o *obs) getStorageClass(storageClass huaweiobs.StorageClassType) string {
	var sc string
	switch storageClass {
	case "":
		sc = string(huaweiobs.StorageClassStandard)
	case huaweiobs.StorageClassWarm:
		sc = OBSStorageClassStandardIA
	default:
		sc = string(storageClass)
	}

	return sc
}
