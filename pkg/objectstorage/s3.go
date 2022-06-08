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
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
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
func (s *s3) GetBucketMetadata(bucketName string) (*BucketMetadata, error) {
	_, err := s.client.HeadBucket(&awss3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		return nil, err
	}

	return &BucketMetadata{
		Name: bucketName,
	}, nil
}

// CreateBucket creates bucket of object storage.
func (s *s3) CreateBucket(bucketName string) error {
	_, err := s.client.CreateBucket(&awss3.CreateBucketInput{Bucket: aws.String(bucketName)})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (s *s3) DeleteBucket(bucketName string) error {
	_, err := s.client.DeleteBucket(&awss3.DeleteBucketInput{Bucket: aws.String(bucketName)})
	return err
}

// DeleteBucket deletes bucket of object storage.
func (s *s3) ListBucketMetadatas() ([]*BucketMetadata, error) {
	resp, err := s.client.ListBuckets(&awss3.ListBucketsInput{})
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
func (s *s3) GetObjectMetadata(bucketName, objectKey string) (*ObjectMetadata, error) {
	resp, err := s.client.HeadObject(&awss3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, err
	}

	return &ObjectMetadata{
		Key:                objectKey,
		ContentDisposition: aws.StringValue(resp.ContentDisposition),
		ContentEncoding:    aws.StringValue(resp.ContentEncoding),
		ContentLanguage:    aws.StringValue(resp.ContentLanguage),
		ContentLength:      aws.Int64Value(resp.ContentLength),
		ContentType:        aws.StringValue(resp.ContentType),
		Etag:               aws.StringValue(resp.ETag),
	}, nil
}

// GetOject returns data of object.
func (s *s3) GetOject(bucketName, objectKey string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(&awss3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

// CreateObject creates data of object.
func (s *s3) CreateObject(bucketName, objectKey string, reader io.Reader) error {
	_, err := s.client.PutObject(&awss3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   aws.ReadSeekCloser(reader),
	})

	return err
}

// DeleteObject deletes data of object.
func (s *s3) DeleteObject(bucketName, objectKey string) error {
	_, err := s.client.DeleteObject(&awss3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})

	return err
}

// DeleteObject deletes data of object.
func (s *s3) ListObjectMetadatas(bucketName, prefix, marker string, limit int64) ([]*ObjectMetadata, error) {
	resp, err := s.client.ListObjects(&awss3.ListObjectsInput{
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
			Etag: aws.StringValue(object.ETag),
		})
	}

	return metadatas, nil
}
