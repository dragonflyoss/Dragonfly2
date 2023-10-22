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
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	registryClient "github.com/docker/distribution/registry/client"
	"github.com/google/uuid"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
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

	// Initialize queues.
	queues := getSchedulerQueues(schedulers)

	// Generate download files.
	var files []internaljob.PreheatRequest
	var err error
	switch PreheatType(json.Type) {
	case PreheatImageType:

		files, err = p.getImageLayers(ctx, json)
		if err != nil {
			return nil, err
		}
	case PreheatFileType:
		files = []internaljob.PreheatRequest{
			{
				URL:     json.URL,
				Tag:     json.Tag,
				Filter:  json.Filter,
				Headers: json.Headers,
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

// getImageLayers gets layers of image.
func (p *preheat) getImageLayers(ctx context.Context, args types.PreheatArgs) ([]internaljob.PreheatRequest, error) {
	ctx, span := tracer.Start(ctx, config.SpanGetLayers, trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	// Parse image manifest url.
	image, err := parseAccessURL(args.URL)
	if err != nil {
		return nil, err
	}

	// init docker auth client
	client, err := NewImageAuthClient(
		image,
		WithClient(&http.Client{Timeout: p.httpRequestTimeout}),
		WithTransport(&http.Transport{TLSClientConfig: &tls.Config{RootCAs: p.rootCAs}}),
		WithBasicAuth(&BasicAuth{Username: args.Username, Password: args.Password}),
	)
	if err != nil {
		return nil, err
	}

	// Parse platform
	platform := platforms.DefaultSpec()
	if args.Platform != "" {
		platform, err = platforms.Parse(args.Platform)
		if err != nil {
			return nil, err
		}
	}

	// Get manifests
	header := nethttp.MapToHeader(args.Headers).Clone()
	manifests, err := p.getManifests(ctx, client, image, header, platform)
	if err != nil {
		return nil, err
	}

	// no matching manifest for platform in the manifest list entries
	if len(manifests) == 0 {
		return nil, noMatchesErr{}
	}

	// set authorization header
	header.Set("Authorization", client.GetBearerToken())

	// prase image layers to preheat
	layers, err := p.parseLayers(manifests, args, header, image)
	if err != nil {
		return nil, err
	}

	return layers, nil
}

// getManifests gets manifests of image.
func (p *preheat) getManifests(ctx context.Context, client *ImageAuthClient, image *preheatImage, header http.Header, pp specs.Platform) ([]distribution.Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, image.buildManifestURL(image.tag), nil)
	if err != nil {
		return nil, err
	}

	req.Header = GetManifestMediaTypeAcceptHeader(header)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return nil, distribution.ErrManifestNotModified
	} else if !registryClient.SuccessStatus(resp.StatusCode) {
		return nil, registryClient.HandleErrorResponse(resp)
	}

	ctHeader := resp.Header.Get("Content-Type")
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	manifest, _, err := distribution.UnmarshalManifest(ctHeader, body)
	if err != nil {
		return nil, err
	}

	switch v := manifest.(type) {
	case *schema1.SignedManifest, *schema2.DeserializedManifest, *ocischema.DeserializedManifest:
		return []distribution.Manifest{v}, nil
	case *manifestlist.DeserializedManifestList:
		callback := func(m manifestlist.ManifestDescriptor) ([]distribution.Manifest, error) {
			image.tag = m.Digest.String()
			return p.getManifests(ctx, client, image, header, pp)
		}

		return p.getManifestsFromManifestList(ctx, v, pp, callback)
	}

	return nil, invalidManifestFormatError{}
}

func (p *preheat) getManifestsFromManifestList(
	ctx context.Context,
	manifestList *manifestlist.DeserializedManifestList,
	pp specs.Platform,
	callback func(manifestlist.ManifestDescriptor) ([]distribution.Manifest, error),
) ([]distribution.Manifest, error) {
	var ms []distribution.Manifest
	for _, v := range p.filterManifests(manifestList.Manifests, pp) {
		manifestList, err := callback(v)
		if err != nil {
			return nil, err
		}

		ms = append(ms, manifestList...)
	}

	return ms, nil
}

// filterManifests
func (p *preheat) filterManifests(manifests []manifestlist.ManifestDescriptor, pp specs.Platform) []manifestlist.ManifestDescriptor {
	var matches []manifestlist.ManifestDescriptor
	for _, desc := range manifests {
		if desc.Platform.Architecture == pp.Architecture && desc.Platform.OS == pp.OS {
			matches = append(matches, desc)
		}
	}

	return matches
}

// parseLayers parses layers of image.
func (p *preheat) parseLayers(manifests []distribution.Manifest, args types.PreheatArgs, header http.Header, image *preheatImage) ([]internaljob.PreheatRequest, error) {
	var layers []internaljob.PreheatRequest
	for _, m := range manifests {
		for _, v := range m.References() {
			h := header.Clone()
			h.Set("Accept", v.MediaType)
			layer := internaljob.PreheatRequest{
				URL:     image.buildBlobsURL(v.Digest.String()),
				Tag:     args.Tag,
				Filter:  args.Filter,
				Headers: nethttp.HeaderToMap(h),
			}

			layers = append(layers, layer)
		}
	}
	return layers, nil
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
