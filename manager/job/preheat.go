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
	"net/url"
	"regexp"
	"time"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
	"github.com/containerd/containerd/platforms"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	registryclient "github.com/docker/distribution/registry/client"
	"github.com/docker/distribution/registry/client/auth"
	"github.com/docker/distribution/registry/client/transport"
	typesregistry "github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/registry"
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

// preheatImage is an image for preheat.
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
	job             *internaljob.Job
	registryTimeout time.Duration
	rootCAs         *x509.CertPool
}

// newPreheat creates a new Preheat.
func newPreheat(job *internaljob.Job, registryTimeout time.Duration, rootCAs *x509.CertPool) (Preheat, error) {
	return &preheat{job, registryTimeout, rootCAs}, nil
}

// CreatePreheat creates a preheat job.
func (p *preheat) CreatePreheat(ctx context.Context, schedulers []models.Scheduler, json types.PreheatArgs) (*internaljob.GroupJobState, error) {
	var span trace.Span
	ctx, span = tracer.Start(ctx, config.SpanPreheat, trace.WithSpanKind(trace.SpanKindProducer))
	span.SetAttributes(config.AttributePreheatType.String(json.Type))
	span.SetAttributes(config.AttributePreheatURL.String(json.URL))
	defer span.End()

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
				URL:                 json.URL,
				Tag:                 json.Tag,
				FilteredQueryParams: json.FilteredQueryParams,
				Headers:             json.Headers,
			},
		}
	default:
		return nil, errors.New("unknown preheat type")
	}

	// Initialize queues.
	queues := getSchedulerQueues(schedulers)
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
	image, err := parseManifestURL(args.URL)
	if err != nil {
		return nil, err
	}

	// Init docker auth client.
	client, err := newImageAuthClient(
		image,
		withHTTPClient(&http.Client{
			Timeout: p.registryTimeout,
			Transport: &http.Transport{
				DialContext:     nethttp.NewSafeDialer().DialContext,
				TLSClientConfig: &tls.Config{RootCAs: p.rootCAs},
			},
		}),
		withBasicAuth(args.Username, args.Password),
	)
	if err != nil {
		return nil, err
	}

	// Get platform.
	platform := platforms.DefaultSpec()
	if args.Platform != "" {
		platform, err = platforms.Parse(args.Platform)
		if err != nil {
			return nil, err
		}
	}

	// Get manifests.
	header := nethttp.MapToHeader(args.Headers)
	manifests, err := p.getManifests(ctx, client, image, header.Clone(), platform)
	if err != nil {
		return nil, err
	}

	// no matching manifest for platform in the manifest list entries
	if len(manifests) == 0 {
		return nil, fmt.Errorf("no matching manifest for platform %s", platforms.Format(platform))
	}

	// set authorization header
	header.Set("Authorization", client.GetAuthToken())

	// prase image layers to preheat
	layers, err := p.parseLayers(manifests, args, header.Clone(), image)
	if err != nil {
		return nil, err
	}

	return layers, nil
}

// getManifests gets manifests of image.
func (p *preheat) getManifests(ctx context.Context, client *imageAuthClient, image *preheatImage, header http.Header, platform specs.Platform) ([]distribution.Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, image.manifestURL(), nil)
	if err != nil {
		return nil, err
	}

	// Set accept header with media types.
	for _, mediaType := range distribution.ManifestMediaTypes() {
		req.Header.Add("Accept", mediaType)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Handle response.
	if resp.StatusCode == http.StatusNotModified {
		return nil, distribution.ErrManifestNotModified
	} else if !registryclient.SuccessStatus(resp.StatusCode) {
		return nil, registryclient.HandleErrorResponse(resp)
	}

	ctHeader := resp.Header.Get("Content-Type")
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Unmarshal manifest.
	manifest, _, err := distribution.UnmarshalManifest(ctHeader, body)
	if err != nil {
		return nil, err
	}

	switch v := manifest.(type) {
	case *schema1.SignedManifest, *schema2.DeserializedManifest, *ocischema.DeserializedManifest:
		return []distribution.Manifest{v}, nil
	case *manifestlist.DeserializedManifestList:
		var result []distribution.Manifest
		for _, v := range p.filterManifests(v.Manifests, platform) {
			image.tag = v.Digest.String()
			manifests, err := p.getManifests(ctx, client, image, header.Clone(), platform)
			if err != nil {
				return nil, err
			}

			result = append(result, manifests...)
		}

		return result, nil
	}

	return nil, errors.New("unknown manifest type")
}

// filterManifests filters manifests with platform.
func (p *preheat) filterManifests(manifests []manifestlist.ManifestDescriptor, platform specs.Platform) []manifestlist.ManifestDescriptor {
	var matches []manifestlist.ManifestDescriptor
	for _, desc := range manifests {
		if desc.Platform.Architecture == platform.Architecture && desc.Platform.OS == platform.OS {
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
			header.Set("Accept", v.MediaType)
			layer := internaljob.PreheatRequest{
				URL:                 image.blobsURL(v.Digest.String()),
				Tag:                 args.Tag,
				FilteredQueryParams: args.FilteredQueryParams,
				Headers:             nethttp.HeaderToMap(header),
			}

			layers = append(layers, layer)
		}
	}

	return layers, nil
}

// imageAuthClientOption is an option for imageAuthClient.
type imageAuthClientOption func(*imageAuthClient)

// withBasicAuth sets basic auth for imageAuthClient.
func withBasicAuth(username, password string) imageAuthClientOption {
	return func(c *imageAuthClient) {
		c.authConfig = &typesregistry.AuthConfig{
			Username: username,
			Password: password,
		}
	}
}

// withHTTPClient sets http client for imageAuthClient.
func withHTTPClient(client *http.Client) imageAuthClientOption {
	return func(c *imageAuthClient) {
		c.httpClient = client
	}
}

// imageAuthClient is a client for image authentication.
type imageAuthClient struct {
	// httpClient is the http client.
	httpClient *http.Client

	// authConfig is the auth config.
	authConfig *typesregistry.AuthConfig

	// interceptorTokenHandler is the token interceptor.
	interceptorTokenHandler *interceptorTokenHandler
}

// newImageAuthClient creates a new imageAuthClient.
func newImageAuthClient(image *preheatImage, opts ...imageAuthClientOption) (*imageAuthClient, error) {
	d := &imageAuthClient{
		httpClient:              http.DefaultClient,
		interceptorTokenHandler: newInterceptorTokenHandler(),
	}

	for _, opt := range opts {
		opt(d)
	}

	// New a challenge manager for the supported authentication types.
	challengeManager, err := registry.PingV2Registry(&url.URL{Scheme: image.protocol, Host: image.domain}, d.httpClient.Transport)
	if err != nil {
		return nil, err
	}

	// New a credential store which always returns the same credential values.
	creds := registry.NewStaticCredentialStore(d.authConfig)

	// Transport with authentication.
	d.httpClient.Transport = transport.NewTransport(
		d.httpClient.Transport,
		auth.NewAuthorizer(
			challengeManager,
			auth.NewTokenHandlerWithOptions(auth.TokenHandlerOptions{
				Transport:   d.httpClient.Transport,
				Credentials: creds,
				Scopes: []auth.Scope{auth.RepositoryScope{
					Repository: image.name,
					Actions:    []string{"pull"},
				}},
				ClientID: registry.AuthClientID,
			}),
			d.interceptorTokenHandler,
			auth.NewBasicHandler(creds),
		),
	)

	return d, nil
}

// Do sends an HTTP request and returns an HTTP response.
func (d *imageAuthClient) Do(req *http.Request) (*http.Response, error) {
	return d.httpClient.Do(req)
}

// GetAuthToken returns the bearer token.
func (d *imageAuthClient) GetAuthToken() string {
	return d.interceptorTokenHandler.GetAuthToken()
}

// interceptorTokenHandler is a token interceptor
// intercept bearer token from auth handler.
type interceptorTokenHandler struct {
	auth.AuthenticationHandler
	token string
}

// NewInterceptorTokenHandler returns a new InterceptorTokenHandler.
func newInterceptorTokenHandler() *interceptorTokenHandler {
	return &interceptorTokenHandler{}
}

// Scheme returns the authentication scheme.
func (h *interceptorTokenHandler) Scheme() string {
	return "bearer"
}

// AuthorizeRequest sets the Authorization header on the request.
func (h *interceptorTokenHandler) AuthorizeRequest(req *http.Request, params map[string]string) error {
	h.token = req.Header.Get("Authorization")
	return nil
}

// GetAuthToken returns the bearer token.
func (h *interceptorTokenHandler) GetAuthToken() string {
	return h.token
}
