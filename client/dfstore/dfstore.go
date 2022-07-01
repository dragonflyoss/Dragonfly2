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

//go:generate mockgen -destination mocks/dfstore_mock.go -source dfstore.go -package mocks

package dfstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"

	"github.com/go-http-utils/headers"

	"d7y.io/dragonfly/v2/client/daemon/objectstorage"
)

// Dfstore is the interface used for object storage.
type Dfstore interface {
	// GetObjectRequestWithContext returns *http.Request of getting object.
	GetObjectRequestWithContext(ctx context.Context, input *GetObjectInput) (*http.Request, error)

	// GetObject returns data of object.
	GetObject(ctx context.Context, input *GetObjectInput) (io.ReadCloser, error)

	// PutObjectRequestWithContext returns *http.Request of putting object.
	PutObjectRequestWithContext(ctx context.Context, input *PutOjectInput) (*http.Request, error)

	// PutObject puts data of object.
	PutObject(ctx context.Context, input *PutOjectInput) error

	// DeleteObjectRequestWithContext returns *http.Request of deleting object.
	DeleteObjectRequestWithContext(ctx context.Context, input *DeleteObjectInput) (*http.Request, error)

	// DeleteObject deletes data of object.
	DeleteObject(ctx context.Context, input *DeleteObjectInput) error

	// IsObjectExistRequestWithContext returns *http.Request of heading object.
	IsObjectExistRequestWithContext(ctx context.Context, input *IsObjectExistInput) (*http.Request, error)

	// IsObjectExist returns whether the object exists.
	IsObjectExist(ctx context.Context, input *IsObjectExistInput) (bool, error)
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

// GetObjectInput is used to construct request of getting object.
type GetObjectInput struct {
	// BucketName is bucket name.
	BucketName string

	// ObjectKey is object key.
	ObjectKey string

	// Filter is used to generate a unique Task ID by
	// filtering unnecessary query params in the URL,
	// it is separated by & character.
	Filter string

	// Range is the HTTP range header.
	Range string
}

// Validate validates GetObjectInput fields.
func (i *GetObjectInput) Validate() error {
	if i.BucketName == "" {
		return errors.New("invalid BucketName")

	}

	if i.ObjectKey == "" {
		return errors.New("invalid ObjectKey")
	}

	return nil
}

// GetObjectRequestWithContext returns *http.Request of getting object.
func (ds *dfstore) GetObjectRequestWithContext(ctx context.Context, input *GetObjectInput) (*http.Request, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	u, err := url.Parse(ds.endpoint)
	if err != nil {
		return nil, err
	}

	u.Path = filepath.Join("buckets", input.BucketName, "objects", input.ObjectKey)

	query := u.Query()
	if input.Filter != "" {
		query.Set("filter", input.Filter)
	}
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	if input.Range != "" {
		req.Header.Set(headers.Range, input.Range)
	}

	return req, nil
}

// GetObject returns data of object.
func (ds *dfstore) GetObject(ctx context.Context, input *GetObjectInput) (io.ReadCloser, error) {
	req, err := ds.GetObjectRequestWithContext(ctx, input)
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

// PutOjectInput is used to construct request of putting object.
type PutOjectInput struct {
	// BucketName is bucket name.
	BucketName string

	// ObjectKey is object key.
	ObjectKey string

	// Filter is used to generate a unique Task ID by
	// filtering unnecessary query params in the URL,
	// it is separated by & character.
	Filter string

	// Mode is the mode in which the backend is written,
	// including WriteBack and AsyncWriteBack.
	Mode int

	// MaxReplicas is the maximum number of
	// replicas of an object cache in seed peers.
	MaxReplicas int

	// Reader is reader of object.
	Reader io.Reader
}

// Validate validates PutOjectInput fields.
func (i *PutOjectInput) Validate() error {
	if i.BucketName == "" {
		return errors.New("invalid BucketName")

	}

	if i.ObjectKey == "" {
		return errors.New("invalid ObjectKey")
	}

	if i.Mode != objectstorage.WriteBack && i.Mode != objectstorage.AsyncWriteBack {
		return errors.New("invalid Mode")
	}

	if i.MaxReplicas < 0 || i.MaxReplicas > 100 {
		return errors.New("invalid MaxReplicas")
	}

	return nil
}

// PutObjectRequestWithContext returns *http.Request of putting object.
func (ds *dfstore) PutObjectRequestWithContext(ctx context.Context, input *PutOjectInput) (*http.Request, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// AsyncWriteBack mode is used by default.
	if err := writer.WriteField("mode", fmt.Sprint(input.Mode)); err != nil {
		return nil, err
	}

	if input.Filter != "" {
		if err := writer.WriteField("filter", input.Filter); err != nil {
			return nil, err
		}
	}

	if input.MaxReplicas > 0 {
		if err := writer.WriteField("maxReplicas", fmt.Sprint(input.MaxReplicas)); err != nil {
			return nil, err
		}
	}

	part, err := writer.CreateFormFile("file", filepath.Base(input.ObjectKey))
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(part, input.Reader); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	u, err := url.Parse(ds.endpoint)
	if err != nil {
		return nil, err
	}

	u.Path = filepath.Join("buckets", input.BucketName, "objects", input.ObjectKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Add(headers.ContentType, writer.FormDataContentType())

	return req, nil
}

// PutObject puts data of object.
func (ds *dfstore) PutObject(ctx context.Context, input *PutOjectInput) error {
	req, err := ds.PutObjectRequestWithContext(ctx, input)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad response status %s", resp.Status)
	}

	return nil
}

// DeleteObjectInput is used to construct request of deleting object.
type DeleteObjectInput struct {
	// BucketName is bucket name.
	BucketName string

	// ObjectKey is object key.
	ObjectKey string
}

// Validate validates DeleteObjectInput fields.
func (i *DeleteObjectInput) Validate() error {
	if i.BucketName == "" {
		return errors.New("invalid BucketName")

	}

	if i.ObjectKey == "" {
		return errors.New("invalid ObjectKey")
	}

	return nil
}

// DeleteObjectRequestWithContext returns *http.Request of deleting object.
func (ds *dfstore) DeleteObjectRequestWithContext(ctx context.Context, input *DeleteObjectInput) (*http.Request, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	u, err := url.Parse(ds.endpoint)
	if err != nil {
		return nil, err
	}

	u.Path = filepath.Join("buckets", input.BucketName, "objects", input.ObjectKey)
	return http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
}

// DeleteObject deletes data of object.
func (ds *dfstore) DeleteObject(ctx context.Context, input *DeleteObjectInput) error {
	req, err := ds.DeleteObjectRequestWithContext(ctx, input)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("bad response status %s", resp.Status)
	}

	return nil
}

// IsObjectExistInput is used to construct request of heading object.
type IsObjectExistInput struct {
	// BucketName is bucket name.
	BucketName string

	// ObjectKey is object key.
	ObjectKey string
}

// Validate validates IsObjectExistInput fields.
func (i *IsObjectExistInput) Validate() error {
	if i.BucketName == "" {
		return errors.New("invalid BucketName")

	}

	if i.ObjectKey == "" {
		return errors.New("invalid ObjectKey")
	}

	return nil
}

// IsObjectExistRequestWithContext returns *http.Request of heading object.
func (ds *dfstore) IsObjectExistRequestWithContext(ctx context.Context, input *IsObjectExistInput) (*http.Request, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}

	u, err := url.Parse(ds.endpoint)
	if err != nil {
		return nil, err
	}

	u.Path = filepath.Join("buckets", input.BucketName, "objects", input.ObjectKey)
	return http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
}

// IsObjectExist returns whether the object exists.
func (ds *dfstore) IsObjectExist(ctx context.Context, input *IsObjectExistInput) (bool, error) {
	req, err := ds.IsObjectExistRequestWithContext(ctx, input)
	if err != nil {
		return false, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	if resp.StatusCode/100 != 2 {
		return false, fmt.Errorf("bad response status %s", resp.Status)
	}

	return true, nil
}
