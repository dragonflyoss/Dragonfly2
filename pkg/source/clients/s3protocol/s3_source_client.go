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

package s3protocol

import (
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-http-utils/headers"

	"d7y.io/dragonfly/v2/pkg/source"
)

const S3Scheme = "s3"

const (
	// AWS Region ID
	region = "awsRegion"
	// AWS Access key ID
	endpoint = "awsEndpoint"
	// AWS Access key ID
	accessKeyID = "awsAccessKeyID"
	// AWS Secret Access Key
	secretAccessKey = "awsSecretAccessKey"
	// AWS Session Token
	sessionToken = "awsSessionToken"

	forcePathStyle = "awsS3ForcePathStyle"
)

var _ source.ResourceClient = (*s3SourceClient)(nil)

func init() {
	source.RegisterBuilder(S3Scheme, source.NewPlainResourceClientBuilder(Builder))
}

func Builder(optionYaml []byte) (source.ResourceClient, source.RequestAdapter, []source.Hook, error) {
	s3Client := &s3SourceClient{}
	return s3Client, s3Client.adaptor, nil, nil
}

// s3SourceClient is an implementation of the interface of source.ResourceClient.
type s3SourceClient struct {
}

func (s *s3SourceClient) adaptor(request *source.Request) *source.Request {
	clonedRequest := request.Clone(request.Context())
	if request.Header.Get(source.Range) != "" {
		clonedRequest.Header.Set(headers.Range, fmt.Sprintf("bytes=%s", request.Header.Get(source.Range)))
		clonedRequest.Header.Del(source.Range)
	}
	return clonedRequest
}

func (s *s3SourceClient) newAWSS3Client(request *source.Request) (*s3.S3, error) {
	cfg := aws.NewConfig().WithCredentials(credentials.NewStaticCredentials(
		request.Header.Get(accessKeyID), request.Header.Get(secretAccessKey), request.Header.Get(sessionToken)))
	session, err := session.NewSession(cfg)
	if err != nil {
		return nil, fmt.Errorf("new aws session failed: %s", err)
	}

	opts := []*aws.Config{cfg.WithEndpoint(request.Header.Get(endpoint))}
	if r := request.Header.Get(region); r != "" {
		opts = append(opts, cfg.WithRegion(r))
	}
	if pathStyle := request.Header.Get(forcePathStyle); strings.ToLower(pathStyle) == "true" {
		opts = append(opts, cfg.WithS3ForcePathStyle(true))
	}

	return s3.New(session, opts...), nil
}

// GetContentLength get length of resource content
// return source.UnknownSourceFileLen if response status is not StatusOK and StatusPartialContent
func (s *s3SourceClient) GetContentLength(request *source.Request) (int64, error) {
	client, err := s.newAWSS3Client(request)
	if err != nil {
		return -1, err
	}
	resp, err := client.HeadObjectWithContext(request.Context(),
		&s3.HeadObjectInput{
			Bucket: aws.String(request.URL.Host),
			Key:    aws.String(request.URL.Path),
			Range:  aws.String(request.Header.Get(headers.Range)),
		})
	if err != nil {
		return -1, err
	}
	return *resp.ContentLength, nil
}

// IsSupportRange checks if resource supports breakpoint continuation
// return false if response status is not StatusPartialContent
func (s *s3SourceClient) IsSupportRange(request *source.Request) (bool, error) {
	// TODO whether all s3 implements support range ?
	return true, nil
}

// IsExpired checks if a resource received or stored is the same.
// return false and non-nil err to prevent the source from exploding if
// fails to get the result, it is considered that the source has not expired
func (s *s3SourceClient) IsExpired(request *source.Request, info *source.ExpireInfo) (bool, error) {
	return false, fmt.Errorf("not implemented") // TODO: Implement
}

// Download downloads from source
func (s *s3SourceClient) Download(request *source.Request) (*source.Response, error) {
	client, err := s.newAWSS3Client(request)
	if err != nil {
		return nil, err
	}

	resp, err := client.GetObjectWithContext(request.Context(),
		&s3.GetObjectInput{
			Bucket: aws.String(request.URL.Host),
			Key:    aws.String(request.URL.Path),
			// TODO more header pass to GetObjectInput
			Range: aws.String(request.Header.Get(headers.Range)),
		})

	if err != nil {
		// TODO parse error details
		return nil, err
	}

	hdr := source.Header{}
	if resp.Expires != nil {
		hdr[headers.Expires] = []string{*resp.Expires}
	}

	if resp.CacheControl != nil {
		hdr[headers.CacheControl] = []string{*resp.CacheControl}
	}

	var contengLength int64 = -1
	if resp.ContentLength != nil {
		contengLength = *resp.ContentLength
	}

	return &source.Response{
		Status:        "OK",
		StatusCode:    http.StatusOK,
		Header:        hdr,
		Body:          resp.Body,
		ContentLength: contengLength,
		Validate: func() error {
			return nil
		},
	}, nil
}

// GetLastModified gets last modified timestamp milliseconds of resource
func (s *s3SourceClient) GetLastModified(request *source.Request) (int64, error) {
	client, err := s.newAWSS3Client(request)
	if err != nil {
		return -1, err
	}
	resp, err := client.HeadObjectWithContext(request.Context(), &s3.HeadObjectInput{
		Bucket: aws.String(request.URL.Host),
		Key:    aws.String(request.URL.Path),
	})
	if err != nil {
		return -1, err
	}
	return resp.LastModified.UnixMilli(), nil
}

func (s *s3SourceClient) List(request *source.Request) (urls []source.URLEntry, err error) {
	client, err := s.newAWSS3Client(request)
	if err != nil {
		return nil, fmt.Errorf("get s3 client: %w", err)
	}
	// if it's an object, just return it.
	isDir, err := s.isDirectory(client, request)
	if err != nil {
		return nil, err
	}
	// if request is a single file, just return
	if !isDir {
		return []source.URLEntry{buildURLEntry(false, request.URL)}, nil
	}

	// list all files
	path := buildListPrefix(request.URL.Path)
	var continuationToken *string
	delimiter := "/"

	for {
		output, err := client.ListObjectsV2WithContext(
			request.Context(),
			&s3.ListObjectsV2Input{
				Bucket:            aws.String(request.URL.Host),
				Prefix:            aws.String(path),
				MaxKeys:           aws.Int64(1000),
				ContinuationToken: continuationToken,
				Delimiter:         &delimiter,
			})
		if err != nil {
			return urls, fmt.Errorf("list s3 object %s/%s: %w", request.URL.Host, path, err)
		}

		for _, object := range output.Contents {
			if *object.Key != *output.Prefix {
				url := *request.URL
				url.Path = addLeadingSlash(*object.Key)
				urls = append(urls, buildURLEntry(false, &url))
			}
		}

		for _, prefix := range output.CommonPrefixes {
			url := *request.URL
			url.Path = addLeadingSlash(*prefix.Prefix)
			urls = append(urls, buildURLEntry(true, &url))
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}
	return urls, nil
}

func (s *s3SourceClient) isDirectory(client *s3.S3, request *source.Request) (bool, error) {
	uPath := buildListPrefix(request.URL.Path)
	delimiter := "/"
	output, err := client.ListObjectsV2WithContext(
		request.Context(),
		&s3.ListObjectsV2Input{
			Bucket:    aws.String(request.URL.Host),
			Prefix:    aws.String(uPath),
			MaxKeys:   aws.Int64(1),
			Delimiter: &delimiter,
		})
	if err != nil {
		return false, fmt.Errorf("list s3 object %s/%s: %w", request.URL.Host, uPath, err)
	}
	if len(output.Contents)+len(output.CommonPrefixes) > 0 {
		return true, nil
	}
	return false, nil
}

func buildURLEntry(isDir bool, url *url.URL) source.URLEntry {
	if isDir {
		url.Path = addTrailingSlash(url.Path)
		list := strings.Split(url.Path, "/")
		return source.URLEntry{URL: url, Name: list[len(list)-2], IsDir: true}
	}
	_, name := filepath.Split(url.Path)
	return source.URLEntry{URL: url, Name: name, IsDir: false}
}

func addLeadingSlash(s string) string {
	if strings.HasPrefix(s, "/") {
		return s
	}
	return "/" + s
}

func addTrailingSlash(s string) string {
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

func buildListPrefix(s string) string {
	s = addTrailingSlash(s)

	// s3 objects id should not start with '/'
	return strings.TrimLeft(s, "/")
}
