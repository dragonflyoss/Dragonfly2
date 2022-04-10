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
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/remotes/docker"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/util/net/httputils"
)

var tracer = otel.Tracer("manager")

type PreheatType string

const (
	PreheatImageType PreheatType = "image"
	PreheatFileType  PreheatType = "file"
	timeout                      = 1 * time.Minute
)

var accessURLPattern, _ = regexp.Compile("^(.*)://(.*)/v2/(.*)/manifests/(.*)")

type Preheat interface {
	CreatePreheat(context.Context, []model.Scheduler, types.PreheatArgs) (*internaljob.GroupJobState, error)
}

type preheat struct {
	job    *internaljob.Job
	bizTag string
}

type preheatImage struct {
	protocol string
	domain   string
	name     string
	tag      string
}

func newPreheat(job *internaljob.Job, bizTag string) (Preheat, error) {
	return &preheat{
		job:    job,
		bizTag: bizTag,
	}, nil
}

func (p *preheat) CreatePreheat(ctx context.Context, schedulers []model.Scheduler, json types.PreheatArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPreheat, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributePreheatType.String(json.Type))
	span.SetAttributes(config.AttributePreheatURL.String(json.URL))
	defer span.End()

	url := json.URL
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

		files, err = p.getLayers(ctx, image, json)
		if err != nil {
			return nil, err
		}
	case PreheatFileType:
		files = []*internaljob.PreheatRequest{
			{
				URL:     url,
				Tag:     p.bizTag,
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

func (p *preheat) getLayers(ctx context.Context, image *preheatImage, preheatArgs types.PreheatArgs) ([]*internaljob.PreheatRequest, error) {
	ctx, span := tracer.Start(ctx, config.SpanGetLayers, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	ocispecManifest, err := p.getManifests(ctx, image, preheatArgs.Username, preheatArgs.Password)
	if err != nil {
		return nil, err
	}
	layers, err := p.parseLayers(ocispecManifest, preheatArgs.URL, preheatArgs.Filter, httputils.MapToHeader(preheatArgs.Headers), image)
	if err != nil {
		return nil, err
	}

	return layers, nil
}

func Fetch(ctx context.Context, f remotes.Fetcher, desc ocispec.Descriptor, dest io.Writer) error {
	r, err := f.Fetch(ctx, desc)
	defer func() {
		closeErr := r.Close()
		logger.Warnf("close fetch reader: %v", closeErr)
	}()
	if err != nil {
		return err
	}
	dgstr := desc.Digest.Algorithm().Digester()
	_, err = io.Copy(io.MultiWriter(dest, dgstr.Hash()), r)
	if err != nil {
		return err
	}
	if dgstr.Digest() != desc.Digest {
		return fmt.Errorf("content mismatch: %s != %s", dgstr.Digest(), desc.Digest)
	}

	return nil
}

// getResolver returns a resolver.
func (p *preheat) getResolver(ctx context.Context, username, password string) remotes.Resolver {
	creds := func(string) (string, string, error) {
		return username, password, nil
	}
	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	options := docker.ResolverOptions{}
	options.Hosts = docker.ConfigureDefaultRegistries(
		docker.WithClient(client),
		docker.WithAuthorizer(docker.NewDockerAuthorizer(
			docker.WithAuthClient(client),
			docker.WithAuthCreds(creds),
		)),
	)
	return docker.NewResolver(options)

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
	var result map[string]interface{}
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

func (p *preheat) getManifests(ctx context.Context, image *preheatImage, username, password string) (ocispec.Manifest, error) {

	resolver := p.getResolver(ctx, username, password)

	// combine image url and tag
	i := fmt.Sprintf("%s/%s:%s", image.domain, image.name, image.tag)

	_, imageDesc, err := resolver.Resolve(ctx, i)
	if err != nil {
		return ocispec.Manifest{}, err
	}
	f, err := resolver.Fetcher(ctx, i)
	if err != nil {
		return ocispec.Manifest{}, err
	}
	r, err := f.Fetch(ctx, imageDesc)
	if err != nil {
		return ocispec.Manifest{}, fmt.Errorf("failed to fetch %s: %w", imageDesc.Digest, err)
	}
	defer r.Close()

	descResponse, err := io.ReadAll(r)
	if err != nil {
		return ocispec.Manifest{}, fmt.Errorf("failed to read %s: %w", imageDesc.Digest, err)
	}

	platform := platforms.Only(platforms.DefaultSpec())
	switch imageDesc.MediaType {
	case images.MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest:

		var manifest ocispec.Manifest
		if err := json.Unmarshal(descResponse, &manifest); err != nil {
			return ocispec.Manifest{}, err
		}

		if manifest.Config.Digest == imageDesc.Digest && (!platform.Match(*manifest.Config.Platform)) {
			return ocispec.Manifest{}, fmt.Errorf("manifest: invalid platform %s: %w", manifest.Config.Platform, err)
		}

		return manifest, nil
	case images.MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
		var idx ocispec.Index
		if err := json.Unmarshal(descResponse, &idx); err != nil {
			return ocispec.Manifest{}, err
		}
		return ocispec.Manifest{
			Versioned:   idx.Versioned,
			MediaType:   idx.MediaType,
			Layers:      idx.Manifests,
			Annotations: idx.Annotations,
		}, nil
	default:
		return ocispec.Manifest{}, fmt.Errorf("unsupported manifest type %s", imageDesc.MediaType)
	}
}

func references(om ocispec.Manifest) []ocispec.Descriptor {
	references := make([]ocispec.Descriptor, 0, 1+len(om.Layers))
	references = append(references, om.Config)
	references = append(references, om.Layers...)
	return references
}

func (p *preheat) parseLayers(om ocispec.Manifest, url, filter string, header http.Header, image *preheatImage) ([]*internaljob.PreheatRequest, error) {

	var layers []*internaljob.PreheatRequest

	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		return nil, err
	}
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
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusUnauthorized {
		return nil, fmt.Errorf("request registry %d", resp.StatusCode)
	}

	layerHeader := header.Clone()
	if resp.StatusCode == http.StatusUnauthorized {
		token, err := getAuthToken(context.Background(), resp.Header)
		if err != nil {
			return nil, err
		}

		layerHeader.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	}

	for _, v := range references(om) {
		digest := v.Digest.String()
		if len(digest) == 0 {
			continue
		}
		layer := &internaljob.PreheatRequest{
			URL:     layerURL(image.protocol, image.domain, image.name, digest),
			Tag:     p.bizTag,
			Filter:  filter,
			Digest:  digest,
			Headers: httputils.HeaderToMap(layerHeader),
		}

		layers = append(layers, layer)
	}

	return layers, nil
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
