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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	nethttp "d7y.io/dragonfly/v2/pkg/net/http"
)

var tracer = otel.Tracer("manager")

type PreheatType string

const (
	// PreheatImageType is image type of preheat job.
	PreheatImageType PreheatType = "image"

	// PreheatFileType is file type of preheat job.
	PreheatFileType PreheatType = "file"
)

const (
	// Timeout is the default timeout of http client.
	timeout = 1 * time.Minute
)

var accessURLPattern, _ = regexp.Compile("^(.*)://(.*)/v2/(.*)/manifests/(.*)")

type Preheat interface {
	CreatePreheat(context.Context, []model.Scheduler, types.PreheatArgs) (*internaljob.GroupJobState, error)
}

type preheat struct {
	job *internaljob.Job
}

type preheatImage struct {
	protocol string
	domain   string
	name     string
	tag      string
}

func newPreheat(job *internaljob.Job) (Preheat, error) {
	return &preheat{
		job: job,
	}, nil
}

func (p *preheat) CreatePreheat(ctx context.Context, schedulers []model.Scheduler, json types.PreheatArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPreheat, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributePreheatType.String(json.Type))
	span.SetAttributes(config.AttributePreheatURL.String(json.URL))
	defer span.End()

	url := json.URL
	tag := json.Tag
	filter := json.Filter
	rawheader := json.Headers

	// Initialize queues
	queues := getSchedulerQueues(schedulers)

	// Generate download files
	var files []*internaljob.PreheatRequest
	switch PreheatType(json.Type) {
	case PreheatImageType:
		// Parse image manifest url
		image, err := parseAccessURL(url)
		if err != nil {
			return nil, err
		}

		files, err = p.getLayers(ctx, url, tag, filter, nethttp.MapToHeader(rawheader), image)
		if err != nil {
			return nil, err
		}
	case PreheatFileType:
		files = []*internaljob.PreheatRequest{
			{
				URL:     url,
				Tag:     tag,
				Filter:  filter,
				Headers: rawheader,
			},
		}
	default:
		return nil, errors.New("unknow preheat type")
	}

	for _, f := range files {
		logger.Infof("preheat %s file url: %v queues: %v", json.URL, f.URL, queues)
	}

	return p.createGroupJob(ctx, files, queues)
}

func (p *preheat) createGroupJob(ctx context.Context, files []*internaljob.PreheatRequest, queues []internaljob.Queue) (*internaljob.GroupJobState, error) {
	signatures := []*machineryv1tasks.Signature{}
	var urls []string
	for i := range files {
		urls = append(urls, files[i].URL)
	}
	for _, queue := range queues {
		for _, file := range files {
			args, err := internaljob.MarshalRequest(file)
			if err != nil {
				logger.Errorf("preheat marshal request: %v, error: %v", file, err)
				continue
			}

			signatures = append(signatures, &machineryv1tasks.Signature{
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

	if _, err := p.job.Server.SendGroupWithContext(ctx, group, 0); err != nil {
		logger.Error("create preheat group job failed", err)
		return nil, err
	}

	logger.Infof("create preheat group job successfully, group uuid: %s， urls: %s", group.GroupUUID, urls)
	return &internaljob.GroupJobState{
		GroupUUID: group.GroupUUID,
		State:     machineryv1tasks.StatePending,
		CreatedAt: time.Now(),
	}, nil
}

func (p *preheat) getLayers(ctx context.Context, url, tag, filter string, header http.Header, image *preheatImage) ([]*internaljob.PreheatRequest, error) {
	ctx, span := tracer.Start(ctx, config.SpanGetLayers, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	resp, err := p.getManifests(ctx, url, header)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		if resp.StatusCode == http.StatusUnauthorized {
			token, err := getAuthToken(ctx, resp.Header)
			if err != nil {
				return nil, err
			}

			bearer := "Bearer " + token
			header.Add("Authorization", bearer)

			resp, err = p.getManifests(ctx, url, header)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("request registry %d", resp.StatusCode)
		}
	}

	layers, err := p.parseLayers(resp, url, tag, filter, header, image)
	if err != nil {
		return nil, err
	}

	return layers, nil
}

func (p *preheat) getManifests(ctx context.Context, url string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header = header
	req.Header.Add("Accept", schema2.MediaTypeManifest)

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *preheat) parseLayers(resp *http.Response, url, tag, filter string, header http.Header, image *preheatImage) ([]*internaljob.PreheatRequest, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	manifest, _, err := distribution.UnmarshalManifest(schema2.MediaTypeManifest, body)
	if err != nil {
		return nil, err
	}

	var layers []*internaljob.PreheatRequest
	for _, v := range manifest.References() {
		digest := v.Digest.String()
		layer := &internaljob.PreheatRequest{
			URL:     layerURL(image.protocol, image.domain, image.name, digest),
			Tag:     tag,
			Filter:  filter,
			Headers: nethttp.HeaderToMap(header),
		}

		layers = append(layers, layer)
	}

	return layers, nil
}

func getAuthToken(ctx context.Context, header http.Header) (string, error) {
	ctx, span := tracer.Start(ctx, config.SpanAuthWithRegistry, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	authURL := authURL(header.Values("WWW-Authenticate"))
	if len(authURL) == 0 {
		return "", errors.New("authURL is empty")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", authURL, nil)
	if err != nil {
		return "", err
	}

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
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

func authURL(wwwAuth []string) string {
	// Bearer realm="<auth-service-url>",service="<service>",scope="repository:<name>:pull"
	if len(wwwAuth) == 0 {
		return ""
	}
	polished := make([]string, 0)
	for _, it := range wwwAuth {
		polished = append(polished, strings.ReplaceAll(it, "\"", ""))
	}
	fileds := strings.Split(polished[0], ",")
	host := strings.Split(fileds[0], "=")[1]
	query := strings.Join(fileds[1:], "&")
	return fmt.Sprintf("%s?%s", host, query)
}

func layerURL(protocol string, domain string, name string, digest string) string {
	return fmt.Sprintf("%s://%s/v2/%s/blobs/%s", protocol, domain, name, digest)
}

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

func getSchedulerQueues(schedulers []model.Scheduler) []internaljob.Queue {
	var queues []internaljob.Queue
	for _, scheduler := range schedulers {
		queue, err := internaljob.GetSchedulerQueue(scheduler.SchedulerClusterID, scheduler.HostName)
		if err != nil {
			continue
		}

		queues = append(queues, queue)
	}

	return queues
}
