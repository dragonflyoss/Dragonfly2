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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/pkg/source"
)

const (
	ociAcceptHeader  = "application/vnd.oci.image.manifest.v1+json"
	jsonAcceptHeader = "application/json"
	scheme           = "oras"
	configFilePath   = "/.singularity/docker-config.json"

	authHeader  = "X-Dragonfly-Oras-Authorization"
	tokenHeader = "X-Dragonfly-Oras-Token"
	blobDigest  = "digest"
)

var client *orasSourceClient

type Blob struct {
	Digest string `json:"digest"`
}

type Manifest struct {
	Layers []Blob `json:"layers"`
}

func init() {
	client = &orasSourceClient{
		httpClient: http.DefaultClient,
	}

	source.RegisterBuilder(scheme,
		source.NewPlainResourceClientBuilder(Builder),
		source.WithDirector(source.NewPlainDirector(Director)))
}

func Builder(optionYaml []byte) (source.ResourceClient, source.RequestAdapter, []source.Hook, error) {
	var httpClient *http.Client
	httpClient, err := source.ParseToHTTPClient(optionYaml)
	if err != nil {
		return nil, nil, nil, err
	}
	client.httpClient = httpClient
	return client, client.adaptor, nil, nil
}

func Director(rawURL *url.URL, urlMeta *commonv1.UrlMeta) error {
	if err := director(rawURL, urlMeta); err != nil {
		return fmt.Errorf("While pulling image from oci registry: %s", err.Error())
	}
	return nil
}

func director(rawURL *url.URL, urlMeta *commonv1.UrlMeta) error {
	// 1. fetch auth info from local user
	auth, err := fetchAuthInfo(rawURL.Host, false)
	if err != nil {
		return fmt.Errorf("error fetch auth info: %s", err.Error())
	}
	var authHdr string
	if auth != "" {
		authHdr = "Basic " + auth
	}

	path, tag, err := parseURL(rawURL.Path)
	if err != nil {
		return fmt.Errorf("error parse url: %s", err.Error())
	}

	ctx := context.TODO()
	host := rawURL.Host

	// 2. fetch token with auth info
	tokenFetchURL := formatTokenURL(host, path)
	token, err := client.fetchTokenWithHeader(ctx, authHdr, tokenFetchURL)
	if err != nil {
		return fmt.Errorf("error fetch token: %s", err.Error())
	}

	// 3. fetch manifest digest, normal is sha256
	digest, err := client.fetchManifest(ctx, host, token, path, tag)
	if err != nil {
		return fmt.Errorf("error fetch manifest: %s", err.Error())
	}

	// 4. update unique blob digest in url
	values := rawURL.Query()
	values.Set(blobDigest, digest)
	rawURL.RawQuery = values.Encode()

	// 5. update digest for peer data check
	urlMeta.Digest = digest

	// 6. update token in header
	urlMeta.Header[tokenHeader] = token

	return nil
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
	resp, err := client.download(request)
	if err != nil {
		return resp, fmt.Errorf("While pulling image from oci registry: %s", err.Error())
	}
	return resp, nil
}

func (client *orasSourceClient) download(request *source.Request) (*source.Response, error) {
	ctx := request.Context()
	host := request.URL.Host

	path, tag, err := parseURL(request.URL.Path)
	if err != nil {
		return nil, fmt.Errorf("error parse url: %s", err.Error())
	}

	var (
		digest string
		token  string
	)

	// if there is blob sha256 and token, just goto fetch image
	if digestQuery, ok1 := request.URL.Query()[blobDigest]; ok1 && len(digestQuery) > 0 {
		if tokenHdr, ok2 := request.Header[tokenHeader]; ok2 && len(tokenHdr) > 0 {
			digest = digestQuery[0]
			token = tokenHdr[0]
			goto fetch
		}
	}

	token, err = client.fetchToken(request, path)
	if err != nil {
		return nil, fmt.Errorf("error fetch token: %s", err.Error())
	}

	digest, err = client.fetchManifest(ctx, host, token, path, tag)
	if err != nil {
		return nil, fmt.Errorf("error fetch manifest: %s", err.Error())
	}

fetch:
	imageFetchResponse, err := client.fetchImage(ctx, host, token, path, digest)
	if err != nil {
		return nil, fmt.Errorf("error fetch image: %s", err.Error())
	}
	return imageFetchResponse, nil
}

func fetchAuthInfo(host string, skipCheckExist bool) (string, error) {
	configFile := os.Getenv("HOME") + configFilePath
	if !skipCheckExist && !fileExists(configFile) {
		return "", nil
	}

	var auth string
	databytes, err := os.ReadFile(configFile)
	if err != nil {
		return "", err
	}

	var jsonData map[string]any
	if err = json.Unmarshal(databytes, &jsonData); err != nil {
		return "", err
	}

	for _, v := range jsonData {
		for registry, v1 := range (v).(map[string]any) {
			for _, credentials := range (v1).(map[string]any) {
				if registry == host {
					auth = credentials.(string)
				}
			}
		}
	}
	return auth, nil
}

func (client *orasSourceClient) fetchToken(request *source.Request, path string) (string, error) {
	tokenFetchURL := formatTokenURL(request.URL.Host, path)

	var authHeaderVal string

	if authHeaderVal = request.Header.Get(authHeader); authHeaderVal != "" {
		// remove the internal auth header
		request.Header.Del(authHeader)
	} else if fileExists(os.Getenv("HOME") + configFilePath) {
		auth, err := fetchAuthInfo(request.URL.Host, true)
		if err != nil {
			return "", err
		}
		authHeaderVal = "Basic " + auth
	}

	token, err := client.fetchTokenWithHeader(request.Context(), authHeaderVal, tokenFetchURL)
	if err != nil {
		return "", err
	}
	logger.Info(fmt.Sprintf("fetching token for %s successfully", request.URL))
	return token, nil
}

func (client *orasSourceClient) fetchTokenWithHeader(ctx context.Context, authHeaderVal, tokenFetchURL string) (string, error) {
	// FIXME always jsonAcceptHeader ?
	var acceptHeader string
	if authHeaderVal != "" {
		acceptHeader = jsonAcceptHeader
	}

	response, err := client.doRequest(ctx, acceptHeader, authHeaderVal, tokenFetchURL)
	if err != nil {
		return "", err
	}
	token, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	var tokenData map[string]any
	if err = json.Unmarshal(token, &tokenData); err != nil {
		return "", err
	}

	tokenVal := fmt.Sprintf("%v", tokenData["token"])
	return tokenVal, nil
}

func (client *orasSourceClient) fetchManifest(ctx context.Context, host, accessToken, path, tag string) (string, error) {
	var sha string
	var blobLayers Manifest
	manifestFetchURL := fmt.Sprintf("https://%s/v2/%s/manifests/%s", host, path, tag)
	authHeaderVal := "Bearer " + accessToken
	resp, err := client.doRequest(ctx, ociAcceptHeader, authHeaderVal, manifestFetchURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	manifest, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if err = json.Unmarshal(manifest, &blobLayers); err != nil {
		return "", err
	}
	for _, value := range blobLayers.Layers {
		sha = value.Digest
	}
	if sha != "" {
		logger.Info(fmt.Sprintf("fetching manifests for %s/%s:%s successfully", host, path, tag))
		return sha, nil
	}
	return "", errors.New("manifest is empty")
}

func (client *orasSourceClient) fetchImage(ctx context.Context, host, token, path, sha256 string) (*source.Response, error) {
	imageFetchURL := fmt.Sprintf("https://%s/v2/%s/blobs/%s", host, path, sha256)
	authHeaderVal := "Bearer " + token
	resp, err := client.doRequest(ctx, ociAcceptHeader, authHeaderVal, imageFetchURL)
	if err != nil {
		return nil, errors.New("failed to fetch image")
	}
	logger.Info(fmt.Sprintf("Fetched image %s/%s with digest %s successfully", host, path, sha256))
	return source.NewResponse(resp.Body), nil
}

func (client *orasSourceClient) doRequest(ctx context.Context, acceptHeaderVal, authHeaderVal, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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

func parseURL(urlPath string) (string, string, error) {
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

func formatTokenURL(host, path string) string {
	return fmt.Sprintf("https://%s/service/token/?scope=repository:%s:pull&service=harbor-registry", host, path)
}
