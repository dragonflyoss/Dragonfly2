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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
)

type s3 struct {
	// S3 client.
	client *awss3.S3
}

// New s3 instance.
func newS3(region, endpoint, accessKey, secretKey string) (ObjectStorage, error) {
	cfg := aws.NewConfig().WithCredentials(credentials.NewStaticCredentials(accessKey, secretKey, ""))
	s, err := session.NewSession(cfg)
	if err != nil {
		return nil, fmt.Errorf("new aws session failed: %s", err)
	}

	return &s3{
		client: awss3.New(s, cfg.WithRegion(region), cfg.WithEndpoint(endpoint)),
	}, nil
}

// GetBucketMetadata returns metadata of bucket.
func (s *s3) GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error) {
	_, err := s.client.HeadBucketWithContext(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		return nil, err
	}

	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (s *s3) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := s.client.CreateBucketWithContext(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucketName)})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (s *s3) DeleteBucket(ctx context.Context, bucketName string) error {
	_, err := s.client.DeleteBucketWithContext(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (s *s3) ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error) {
	resp, err := s.client.ListBucketsWithContext(ctx, &awss3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var metadatas []*BucketMetadata
	for _, bucket := range resp.Buckets {
		metadatas = append(metadatas, &BucketMetadata{
			Name:     aws.StringValue(bucket.Name),
			CreateAt: aws.TimeValue(bucket.CreationDate),
		})
	}

	return metadatas, nil
}

// GetObjectMetadata returns metadata of object.
func (s *s3) GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error) {
	resp, err := s.client.HeadObjectWithContext(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		// S3 is missing this error code.
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return nil, false, nil
		}

		return nil, false, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: aws.StringValue(resp.ContentDisposition),
		ContentEncoding:    aws.StringValue(resp.ContentEncoding),
		ContentLanguage:    aws.StringValue(resp.ContentLanguage),
		ContentLength:      aws.Int64Value(resp.ContentLength),
		ContentType:        aws.StringValue(resp.ContentType),
		ETag:               aws.StringValue(resp.ETag),
		Digest:             aws.StringValue(resp.Metadata[MetaDigest]),
	}, true, nil
}

// GetOject returns data of object.
func (s *s3) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	resp, err := s.client.GetObjectWithContext(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// CreateObject creates data of object.
func (s *s3) CreateObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
	meta := map[string]string{}
	meta[MetaDigest] = digest

	_, err := s.client.PutObjectWithContext(ctx, &awss3.PutObjectInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		Body:     aws.ReadSeekCloser(reader),
		Metadata: aws.StringMap(meta),
	})

	return err
}

// DeleteObject deletes data of object.
func (s *s3) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
	_, err := s.client.DeleteObjectWithContext(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})

	return err
}

// DeleteObject deletes data of object.
func (s *s3) ListObjectMetadatas(ctx context.Context, bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error) {
	resp, err := s.client.ListObjectsWithContext(ctx, &awss3.ListObjectsInput{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(prefix),
		Marker:  aws.String(marker),
		MaxKeys: aws.Int64(limit),
	})
	if err != nil {
		return nil, err
	}

	var metadatas []*ObjectMetadata
	for _, object := range resp.Contents {
		metadatas = append(metadatas, &ObjectMetadata{
			Key:  aws.StringValue(object.Key),
			ETag: aws.StringValue(object.ETag),
		})
	}

	return metadatas, nil
}

// IsObjectExist returns whether the object exists.
func (s *s3) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
	_, isExist, err := s.GetObjectMetadata(ctx, bucketName, objectKey)
	if err != nil {

		return false, err
	}

	if !isExist {
		return false, nil
	}

	return true, nil
}

// GetSignURL returns sign url of object.
func (s *s3) GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error) {
	var req *request.Request
	switch method {
	case MethodGet:
		req, _ = s.client.GetObjectRequest(&awss3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	case MethodPut:
		req, _ = s.client.PutObjectRequest(&awss3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	case MethodHead:
		req, _ = s.client.HeadObjectRequest(&awss3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	case MethodDelete:
		req, _ = s.client.DeleteObjectRequest(&awss3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
	case MethodList:
		req, _ = s.client.ListObjectsRequest(&awss3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
	default:
		return "", fmt.Errorf("not support method %s", method)
	}

	return req.Presign(expire)
}
