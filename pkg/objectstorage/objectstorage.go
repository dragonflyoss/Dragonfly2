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

//go:generate mockgen -destination mocks/objectstorage_mock.go -source objectstorage.go -package mocks

package objectstorage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ObjectMetadata provides metadata of object.
type ObjectMetadata struct {
	// Key is object key.
	Key string

	// ContentDisposition is Content-Disposition header.
	ContentDisposition string

	// ContentEncoding is Content-Encoding header.
	ContentEncoding string

	// ContentLanguage is Content-Language header.
	ContentLanguage string

	// ContentLanguage is Content-Length header.
	ContentLength int64

	// ContentType is Content-Type header.
	ContentType string

	// ETag is ETag header.
	ETag string

	// Digest is object digest.
	Digest string

	// LastModifiedTime is last modified time.
	LastModifiedTime time.Time

	// StorageClass is object storage class.
	StorageClass string
}

// ObjectMetadatas provides metadatas of object.
type ObjectMetadatas struct {
	// CommonPrefixes are similar prefixes in object storage.
	CommonPrefixes []string `json:"CommonPrefixes"`

	// Metadatas are metadata of objects.
	Metadatas []*ObjectMetadata `json:"Metadatas"`
}

// Metadata provides metadata of object storage.
type Metadata struct {
	// Name is object storage name of type, it can be s3, oss or obs.
	Name string

	// Region is storage region.
	Region string

	// Endpoint is datacenter endpoint.
	Endpoint string
}

// BucketMetadata provides metadata of bucket.
type BucketMetadata struct {
	// Name is bucket name.
	Name string

	// CreateAt is bucket create time.
	CreateAt time.Time
}

// ObjectStorage is the interface used for object storage.
type ObjectStorage interface {
	// GetMetadata returns metadata of object storage.
	GetMetadata(ctx context.Context) *Metadata

	// GetBucketMetadata returns metadata of bucket.
	GetBucketMetadata(ctx context.Context, bucketName string) (*BucketMetadata, error)

	// CreateBucket creates bucket of object storage.
	CreateBucket(ctx context.Context, bucketName string) error

	// DeleteBucket deletes bucket of object storage.
	DeleteBucket(ctx context.Context, bucketName string) error

	// ListBucketMetadatas returns metadata of buckets.
	ListBucketMetadatas(ctx context.Context) ([]*BucketMetadata, error)

	// IsBucketExist returns whether the bucket exists.
	IsBucketExist(ctx context.Context, bucketName string) (bool, error)

	// GetObjectMetadata returns metadata of object.
	GetObjectMetadata(ctx context.Context, bucketName, objectKey string) (*ObjectMetadata, bool, error)

	// GetObjectMetadatas returns the metadata of the objects.
	GetObjectMetadatas(ctx context.Context, bucketName, prefix, marker, delimiter string, limit int64) (*ObjectMetadatas, error)

	// GetOject returns data of object.
	GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error)

	// PutObject puts data of object.
	PutObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error

	// DeleteObject deletes data of object.
	DeleteObject(ctx context.Context, bucketName, objectKey string) error

	// IsObjectExist returns whether the object exists.
	IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error)

	// CopyObject copy object from source to destination.
	CopyObject(ctx context.Context, bucketName, sourceObjectKey, destinationObjectKey string) error

	// GetSignURL returns sign url of object.
	GetSignURL(ctx context.Context, bucketName, objectKey string, method Method, expire time.Duration) (string, error)
}

// objectStorage provides object storage.
type objectStorage struct {
	// name is object storage name of type, it can be s3, oss or obs.
	name string

	// region is storage region.
	region string

	// endpoint is datacenter endpoint.
	endpoint string

	// accessKey is access key ID.
	accessKey string

	// secretKey is access key secret.
	secretKey string

	// secretKey is access key secret.
	s3ForcePathStyle bool

	// httpClient is http client.
	httpClient *http.Client
}

// Option is a functional option for configuring the objectStorage.
type Option func(o *objectStorage)

// WithS3ForcePathStyle set the S3ForcePathStyle for objectStorage.
func WithS3ForcePathStyle(s3ForcePathStyle bool) Option {
	return func(o *objectStorage) {
		o.s3ForcePathStyle = s3ForcePathStyle
	}
}

// WithHTTPClient set the http client for objectStorage.
func WithHTTPClient(client *http.Client) Option {
	return func(o *objectStorage) {
		o.httpClient = client
	}
}

// New object storage interface.
func New(name, region, endpoint, accessKey, secretKey string, options ...Option) (ObjectStorage, error) {
	o := &objectStorage{
		name:             name,
		region:           region,
		endpoint:         endpoint,
		accessKey:        accessKey,
		secretKey:        secretKey,
		s3ForcePathStyle: DefaultS3ForcePathStyle,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSHandshakeTimeout:   DefaultTLSHandshakeTimeout,
				ResponseHeaderTimeout: DefaultResponseHeaderTimeout,
				IdleConnTimeout:       DefaultIdleConnTimeout,
				MaxIdleConnsPerHost:   DefaultMaxIdleConnsPerHost,
				ReadBufferSize:        DefaultReadBufferSize,
				WriteBufferSize:       DefaultWriteBufferSize,
				DisableCompression:    DefaultDisableCompression,
			},
			Timeout: DefaultTimeout,
		},
	}

	for _, opt := range options {
		opt(o)
	}

	switch o.name {
	case ServiceNameS3:
		return newS3(o.region, o.endpoint, o.accessKey, o.secretKey, o.s3ForcePathStyle, o.httpClient)
	case ServiceNameOSS:
		return newOSS(o.region, o.endpoint, o.accessKey, o.secretKey, o.httpClient)
	case ServiceNameOBS:
		return newOBS(o.region, o.endpoint, o.accessKey, o.secretKey, o.httpClient)
	}

	return nil, fmt.Errorf("unknow service name %s", name)
}
