/*
 *     Copyright 2024 The Dragonfly Authors
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

package http

import (
	"strings"

	"d7y.io/dragonfly/v2/pkg/idgen"
	pkgstrings "d7y.io/dragonfly/v2/pkg/strings"
)

// S3FilteredQueryParams is the default filtered query params with s3 protocol to generate the task id.
var S3FilteredQueryParams = []string{
	"X-Amz-Algorithm",
	"X-Amz-Credential",
	"X-Amz-Date",
	"X-Amz-Expires",
	"X-Amz-SignedHeaders",
	"X-Amz-Signature",
	"X-Amz-Security-Token",
	"X-Amz-User-Agent",
}

// GCSFilteredQueryParams is the default filtered query params with gcs protocol to generate the task id.
var GCSFilteredQueryParams = []string{
	"X-Goog-Algorithm",
	"X-Goog-Credential",
	"X-Goog-Date",
	"X-Goog-Expires",
	"X-Goog-SignedHeaders",
	"X-Goog-Signature",
}

// OSSFilteredQueryParams is the default filtered query params with oss protocol to generate the task id.
var OSSFilteredQueryParams = []string{
	"OSSAccessKeyId",
	"Expires",
	"Signature",
	"SecurityToken",
}

// OBSFilteredQueryParams is the default filtered query params with obs protocol to generate the task id.
var OBSFilteredQueryParams = []string{
	"AccessKeyId",
	"Signature",
	"Expires",
	"X-Obs-Date",
	"X-Obs-Security-Token",
}

// COSFilteredQueryParams is the default filtered query params with cos protocol to generate the task id.
var COSFilteredQueryParams = []string{
	"q-sign-algorithm",
	"q-ak",
	"q-sign-time",
	"q-key-time",
	"q-header-list",
	"q-url-param-list",
	"q-signature",
	"x-cos-security-token",
}

// ContainerdQueryParams is the default filtered query params with containerd to generate the task id.
var ContainerdQueryParams = []string{
	"ns",
}

// DefaultFilteredQueryParams is the default filtered query params to generate the task id.
var DefaultFilteredQueryParams = pkgstrings.Concat(S3FilteredQueryParams, GCSFilteredQueryParams, OSSFilteredQueryParams, OBSFilteredQueryParams, COSFilteredQueryParams, ContainerdQueryParams)

// RawDefaultFilteredQueryParams is the raw default filtered query params to generate the task id.
var RawDefaultFilteredQueryParams = strings.Join(DefaultFilteredQueryParams, idgen.FilteredQueryParamsSeparator)
