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

package dfstore

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
)

// Dfstore is the interface used for object storage.
type Dfstore interface {
	// GetOject returns data of object.
	GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error)

	// CreateObject creates data of object.
	CreateObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error

	// DeleteObject deletes data of object.
	DeleteObject(ctx context.Context, bucketName, objectKey string) error

	// IsObjectExist returns whether the object exists.
	IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error)
}

// dfstore provides object storage function.
type dfstore struct {
	endpoint   string
	accessKey  string
	secretKey  string
	httpClient *http.Client
}

// Option is a functional option for configuring the dfstore.
type Option func(ds *dfstore)

// WithHTTPClient set http client for dfstore.
func WithHTTPClient(client *http.Client) Option {
	return func(ds *dfstore) {
		ds.httpClient = client
	}
}

// New dfstore instance.
func New(endpoint, accessKey, secretKey string, options ...Option) Dfstore {
	ds := &dfstore{
		endpoint:   endpoint,
		accessKey:  accessKey,
		secretKey:  secretKey,
		httpClient: http.DefaultClient,
	}

	for _, opt := range options {
		opt(ds)
	}

	return ds
}

// GetOject returns data of object.
func (ds *dfstore) GetOject(ctx context.Context, bucketName, objectKey string) (io.ReadCloser, error) {
	u, err := url.Parse(ds.endpoint)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join("buckets", bucketName, "objects", objectKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := ds.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("bad response status %s", resp.Status)
	}

	return resp.Body, nil
}

// CreateObject creates data of object.
func (ds *dfstore) CreateObject(ctx context.Context, bucketName, objectKey, digest string, reader io.Reader) error {
}

// DeleteObject deletes data of object.
func (ds *dfstore) DeleteObject(ctx context.Context, bucketName, objectKey string) error {
}

// IsObjectExist returns whether the object exists.
func (ds *dfstore) IsObjectExist(ctx context.Context, bucketName, objectKey string) (bool, error) {
}
