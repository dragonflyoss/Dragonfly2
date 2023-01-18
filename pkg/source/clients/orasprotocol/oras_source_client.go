/*
 *     Copyright 2023 The Dragonfly Authors
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

package orasprotocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/source"
)

const (
	ociAcceptHeader  = "application/vnd.oci.image.manifest.v1+json"
	jsonAcceptHeader = "application/json"
	scheme           = "oras"
	authFilePath     = "/.singularity/docker-config.json"
)

var _ source.ResourceClient = (*orasSourceClient)(nil)

type Blob struct {
	Digest string `json:"digest"`
}

type Manifest struct {
	Layers []Blob `json:"layers"`
}

func init() {
	source.RegisterBuilder(scheme, source.NewPlainResourceClientBuilder(Builder))
}

func Builder(optionYaml []byte) (source.ResourceClient, source.RequestAdapter, []source.Hook, error) {
	var httpClient *http.Client
	httpClient, err := source.ParseToHTTPClient(optionYaml)
	if err != nil {
		return nil, nil, nil, err
	}
	client := &orasSourceClient{
		httpClient: httpClient,
	}
	return client, client.adaptor, nil, nil
}

func (client *orasSourceClient) adaptor(request *source.Request) *source.Request {
	clonedRequest := request.Clone(request.Context())
	return clonedRequest
}

type orasSourceClient struct {
	httpClient *http.Client
}

func (client *orasSourceClient) GetContentLength(request *source.Request) (int64, error) {
	return -1, nil
}

func (client *orasSourceClient) IsSupportRange(request *source.Request) (bool, error) {
	return false, nil
}

func (client *orasSourceClient) IsExpired(request *source.Request, info *source.ExpireInfo) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (client *orasSourceClient) Download(request *source.Request) (*source.Response, error) {
	path, tag, err := parseURL(request, request.URL.Path)
	if err != nil {
		return nil, err
	}

	token, err := client.fetchToken(request, path)
	if err != nil {
		return nil, err
	}

	sha256, err := client.fetchManifest(request, token, path, tag)
	if err != nil {
		return nil, err
	}

	imageFetchResponse, err := client.fetchImage(request, token, sha256, path, tag)
	if err != nil {
		return nil, err
	}
	return imageFetchResponse, nil
}

func (client *orasSourceClient) fetchToken(request *source.Request, path string) (string, error) {
	var response *http.Response
	var err error
	tokenFetchURL := fmt.Sprintf("https://%s/service/token/?scope=repository:%s:pull&service=harbor-registry", request.URL.Host, path)
	if fileExists(os.Getenv("HOME") + authFilePath) {
		var auth string
		databytes, err := os.ReadFile(os.Getenv("HOME") + authFilePath)
		if err != nil {
			return "", err
		}
		var jsonData map[string]interface{}
		if err := json.Unmarshal(databytes, &jsonData); err != nil {
			return "", err
		}
		for _, v := range jsonData {
			for registry, v1 := range (v).(map[string]interface{}) {
				for _, credentials := range (v1).(map[string]interface{}) {
					if registry == request.URL.Host {
						auth = credentials.(string)
					}
				}
			}
		}
		authHeaderVal := "Basic " + auth
		response, err = client.doRequest(request, jsonAcceptHeader, authHeaderVal, tokenFetchURL)
		if err != nil {
			return "", err
		}
	} else {
		response, err = client.doRequest(request, "", "", tokenFetchURL)
		if err != nil {
			return "", err
		}
	}
	token, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	var tokenData map[string]interface{}
	if err := json.Unmarshal(token, &tokenData); err != nil {
		return "", err
	}
	logger.Info(fmt.Sprintf("fetching token for %s  successful", request.URL))
	tokenVal := fmt.Sprintf("%v", tokenData["token"])
	return tokenVal, nil
}

func (client *orasSourceClient) fetchManifest(request *source.Request, accessToken, path, tag string) (string, error) {
	var sha string
	var blobLayers Manifest
	manifestFetchURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", request.URL.Host, path, tag)
	authHeaderVal := "Bearer " + accessToken
	resp, err := client.doRequest(request, ociAcceptHeader, authHeaderVal, manifestFetchURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	manifest, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal(manifest, &blobLayers); err != nil {
		return "", err
	}
	for _, value := range blobLayers.Layers {
		sha = value.Digest
	}
	if sha != "" {
		logger.Info(fmt.Sprintf("fetching manifests for %s  successful", request.URL))
		return sha, nil
	}
	return "", errors.New("manifest is empty")
}

func (client *orasSourceClient) fetchImage(request *source.Request, token, sha256, path, tag string) (*source.Response, error) {
	imageFetchURL := fmt.Sprintf("https://%s/v2/%s/blobs/%s", request.URL.Host, path, sha256)
	authHeaderVal := "Bearer " + token
	resp, err := client.doRequest(request, ociAcceptHeader, authHeaderVal, imageFetchURL)
	if err != nil {
		return nil, errors.New("failed to fetch image")
	}
	logger.Info(fmt.Sprintf("Fetched %s image successfully", request.URL))
	return source.NewResponse(resp.Body), nil
}

func (client *orasSourceClient) doRequest(request *source.Request, acceptHeaderVal, authHeaderVal, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(request.Context(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if authHeaderVal != "" {
		req.Header.Add("Accept", acceptHeaderVal)
		req.Header.Add("Authorization", authHeaderVal)
	}
	resp, err := client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func fileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func parseURL(request *source.Request, urlPath string) (string, string, error) {
	parseURLPattern, err := regexp.Compile("(.*):(.*)")
	if err != nil {
		return "", "", err
	}
	r := parseURLPattern.FindStringSubmatch(urlPath)
	if len(r) != 3 {
		return "", "", errors.New("Failed to parse URL")
	}
	path := strings.TrimPrefix(r[1], "/")
	tag := r[2]
	return path, tag, nil
}

func (client *orasSourceClient) GetLastModified(request *source.Request) (int64, error) {
	panic("implement me")
}
