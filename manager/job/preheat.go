/*
 *     Copyright 2020 The Dragonfly Authors
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

//go:generate mockgen -destination mocks/preheat_mock.go -source preheat.go -package mocks

package job

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/manifest/schema2"
	"github.com/go-http-utils/headers"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
)

type PreheatType string

const (
	// PreheatImageType is image type of preheat job.
	PreheatImageType PreheatType = "image"

	// PreheatFileType is file type of preheat job.
	PreheatFileType PreheatType = "file"
)

// accessURLPattern is the pattern of access url.
var accessURLPattern, _ = regexp.Compile("^(.*)://(.*)/v2/(.*)/manifests/(.*)")

// Preheat is an interface for preheat job.
type Preheat interface {
	// CreatePreheat creates a preheat job.
	CreatePreheat(context.Context, []models.Scheduler, types.PreheatArgs) (*internaljob.GroupJobState, error)
}

// preheat is an implementation of Preheat.
type preheat struct {
	job                *internaljob.Job
	httpRequestTimeout time.Duration
	rootCAs            *x509.CertPool
}

// preheatImage is image information for preheat.
type preheatImage struct {
	protocol string
	domain   string
	name     string
	tag      string
}

// newPreheat creates a new Preheat.
func newPreheat(job *internaljob.Job, httpRequestTimeout time.Duration, rootCAs *x509.CertPool) (Preheat, error) {
	return &preheat{job, httpRequestTimeout, rootCAs}, nil
}

// CreatePreheat creates a preheat job.
func (p *preheat) CreatePreheat(ctx context.Context, schedulers []models.Scheduler, json types.PreheatArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPreheat, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributePreheatType.String(json.Type))
	span.SetAttributes(config.AttributePreheatURL.String(json.URL))
	defer span.End()

	url := json.URL
	tag := json.Tag
	filter := json.Filter
	rawheader := json.Headers

	// Initialize queues.
	queues := getSchedulerQueues(schedulers)

	// Generate download files.
	var files []internaljob.PreheatRequest
	switch PreheatType(json.Type) {
	case PreheatImageType:
		// Parse image manifest url.
		image, err := parseAccessURL(url)
		if err != nil {
			return nil, err
		}

		files, err = p.getLayers(ctx, url, tag, filter, nethttp.MapToHeader(rawheader), image)
		if err != nil {
			return nil, err
		}
	case PreheatFileType:
		files = []internaljob.PreheatRequest{
			{
				URL:     url,
				Tag:     tag,
				Filter:  filter,
				Headers: rawheader,
			},
		}
	default:
		return nil, errors.New("unknown preheat type")
	}

	return p.createGroupJob(ctx, files, queues)
}

// createGroupJob creates a group job.
func (p *preheat) createGroupJob(ctx context.Context, files []internaljob.PreheatRequest, queues []internaljob.Queue) (*internaljob.GroupJobState, error) {
	var signatures []*machineryv1tasks.Signature
	for _, queue := range queues {
		for _, file := range files {
			args, err := internaljob.MarshalRequest(file)
			if err != nil {
				logger.Errorf("preheat marshal request: %v, error: %v", file, err)
				continue
			}

			signatures = append(signatures, &machineryv1tasks.Signature{
				UUID:       fmt.Sprintf("task_%s", uuid.New().String()),
				Name:       internaljob.PreheatJob,
				RoutingKey: queue.String(),
				Args:       args,
			})
		}
	}

	group, err := machineryv1tasks.NewGroup(signatures...)
	if err != nil {
		return nil, err
	}

	var tasks []machineryv1tasks.Signature
	for _, signature := range signatures {
		tasks = append(tasks, *signature)
	}

	logger.Infof("create preheat group %s in queues %v, tasks: %#v", group.GroupUUID, queues, tasks)
	if _, err := p.job.Server.SendGroupWithContext(ctx, group, 0); err != nil {
		logger.Errorf("create preheat group %s failed", group.GroupUUID, err)
		return nil, err
	}

	return &internaljob.GroupJobState{
		GroupUUID: group.GroupUUID,
		State:     machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}

// getLayers gets layers of image.
func (p *preheat) getLayers(ctx context.Context, url, tag, filter string, header http.Header, image *preheatImage) ([]internaljob.PreheatRequest, error) {
	ctx, span := tracer.Start(ctx, config.SpanGetLayers, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	resp, err := p.getManifests(ctx, url, header, p.httpRequestTimeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		if resp.StatusCode == http.StatusUnauthorized {
			token, err := getAuthToken(ctx, resp.Header, p.httpRequestTimeout, p.rootCAs)
			if err != nil {
				return nil, err
			}

			header.Add(headers.Authorization, fmt.Sprintf("Bearer %s", token))
			resp, err = p.getManifests(ctx, url, header, p.httpRequestTimeout)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("request registry %d", resp.StatusCode)
		}
	}

	layers, err := p.parseLayers(resp, tag, filter, header, image)
	if err != nil {
		return nil, err
	}

	return layers, nil
}

// getManifests gets manifests of image.
func (p *preheat) getManifests(ctx context.Context, url string, header http.Header, timeout time.Duration) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header = header
	req.Header.Add(headers.Accept, schema2.MediaTypeManifest)

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext:     nethttp.NewSafeDialer().DialContext,
			TLSClientConfig: &tls.Config{RootCAs: p.rootCAs},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// parseLayers parses layers of image.
func (p *preheat) parseLayers(resp *http.Response, tag, filter string, header http.Header, image *preheatImage) ([]internaljob.PreheatRequest, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	manifest, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, body)
	if err != nil {
		return nil, err
	}

	var layers []internaljob.PreheatRequest
	for _, v := range manifest.References() {
		layer := internaljob.PreheatRequest{
			URL:     layerURL(image.protocol, image.domain, image.name, v.Digest.String()),
			Tag:     tag,
			Filter:  filter,
			Headers: nethttp.HeaderToMap(header),
		}

		layers = append(layers, layer)
	}

	return layers, nil
}

// getAuthToken gets auth token from registry.
func getAuthToken(ctx context.Context, header http.Header, timeout time.Duration, rootCAs *x509.CertPool) (string, error) {
	ctx, span := tracer.Start(ctx, config.SpanAuthWithRegistry, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	authURL := authURL(header.Values(headers.WWWAuthenticate))
	if len(authURL) == 0 {
		return "", errors.New("authURL is empty")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authURL, nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext:     nethttp.NewSafeDialer().DialContext,
			TLSClientConfig: &tls.Config{RootCAs: rootCAs},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}

	if result["token"] == nil {
		return "", errors.New("token is empty")
	}

	token := fmt.Sprintf("%v", result["token"])
	return token, nil
}

// authURL gets auth url from www-authenticate header.
func authURL(wwwAuth []string) string {
	// Bearer realm="<auth-service-url>",service="<service>",scope="repository:<name>:pull"
	if len(wwwAuth) == 0 {
		return ""
	}

	polished := make([]string, 0)
	for _, it := range wwwAuth {
		polished = append(polished, strings.ReplaceAll(it, "\"", ""))
	}

	fields := strings.Split(polished[0], ",")
	host := strings.Split(fields[0], "=")[1]
	query := strings.Join(fields[1:], "&")
	return fmt.Sprintf("%s?%s", host, query)
}

// layerURL gets layer url.
func layerURL(protocol string, domain string, name string, digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", protocol, domain, name, digest)
}

// parseAccessURL parses access url.
func parseAccessURL(url string) (*preheatImage, error) {
	r := accessURLPattern.FindStringSubmatch(url)
	if len(r) != 5 {
		return nil, errors.New("parse access url failed")
	}

	return &preheatImage{
		protocol: r[1],
		domain:   r[2],
		name:     r[3],
		tag:      r[4],
	}, nil
}
