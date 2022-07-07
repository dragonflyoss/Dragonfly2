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

package cmd

import (
	"errors"
	"fmt"
	"net/url"
)

// Parse object storage url.
func parseDfstoreURL(rawURL string) (string, string, error) {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return "", "", err
	}

	if u.Scheme != DfstoreScheme {
		return "", "", fmt.Errorf("invalid scheme, e.g. %s://bucket_name/object_key", DfstoreScheme)
	}

	if u.Host == "" {
		return "", "", errors.New("invalid bucket name")
	}

	if u.Path == "" {
		return "", "", errors.New("invalid object key")
	}

	return u.Host, u.Path, nil
}

// isDfstoreURL determines whether the raw url is dfstore url.
func isDfstoreURL(rawURL string) bool {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return false
	}

	if u.Scheme != DfstoreScheme || u.Host == "" || u.Path == "" {
		return false
	}

	return true
}
