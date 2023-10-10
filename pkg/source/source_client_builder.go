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

package source

import (
	"fmt"
	"net/url"

	"gopkg.in/yaml.v3"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
)

var (
	resourceClientBuilder = map[string]ResourceClientBuilder{}
	resourceClientOptions = map[string]any{}
	resourceDirector      = map[string]Director{}
)

// ResourceClientBuilder is used to build resource client with custom option
type ResourceClientBuilder interface {
	// Build return the target resource with custom option
	Build(optionYaml []byte) (resourceClient ResourceClient, adaptor RequestAdapter, hooks []Hook, err error)
}

// Director will handle request with some actions, like:
// 1. inject auth information for target url and metadata, eg: fetch docker config for different users
// 2. rewrite a common request into an unique request, eg: oras://harbor/user:latest to oras://harbor/user:lastest?digest=sha256:12345
type Director interface {
	Direct(rawURL *url.URL, urlMeta *commonv1.UrlMeta) error
}

// RegisterOption is used for extra options when registering, like mark target scheme protocol should inject auth information
type RegisterOption func(scheme string)

// RegisterBuilder register ResourceClientBuilder into global resourceClientBuilder, the InitSourceClients will use it.
func RegisterBuilder(scheme string, builder ResourceClientBuilder, opts ...RegisterOption) {
	if _, ok := resourceClientBuilder[scheme]; ok {
		panic(fmt.Sprintf("duplicate ResourceClientBuilder: %s", scheme))
	}
	resourceClientBuilder[scheme] = builder

	for _, opt := range opts {
		opt(scheme)
	}
}

func WithDirector(director Director) RegisterOption {
	return func(scheme string) {
		resourceDirector[scheme] = director
	}
}

func HasDirector(scheme string) (Director, bool) {
	director, ok := resourceDirector[scheme]
	return director, ok
}

func UnRegisterBuilder(scheme string) {
	if _, ok := resourceClientBuilder[scheme]; !ok {
		panic(fmt.Sprintf("scheme ResourceClientBuilder %s not found", scheme))
	}
	delete(resourceClientBuilder, scheme)
}

// InitSourceClients will initialize all resource clients which registered by RegisterBuilder.
func InitSourceClients(opts map[string]any) error {
	// save options for resource plugin
	resourceClientOptions = opts

	for scheme, builder := range resourceClientBuilder {
		var (
			opt []byte
			err error
		)
		if data, ok := resourceClientOptions[scheme]; ok {
			opt, err = yaml.Marshal(data)
			if err != nil {
				return err
			}
		}
		resourceClient, adaptor, hooks, err := builder.Build(opt)
		if err != nil {
			return fmt.Errorf("build resource client %s error: %s, options: %s", scheme, err, string(opt))
		}
		err = _defaultManager.Register(scheme, resourceClient, adaptor, hooks...)
		if err != nil {
			return fmt.Errorf("register resource client %s error: %s, options: %s", scheme, err, string(opt))
		}
	}
	return nil
}

type plainResourceClientBuilder struct {
	build func(optionYaml []byte) (resourceClient ResourceClient, adaptor RequestAdapter, hooks []Hook, err error)
}

func (b *plainResourceClientBuilder) Build(optionYaml []byte) (resourceClient ResourceClient, adaptor RequestAdapter, hooks []Hook, err error) {
	return b.build(optionYaml)
}

func NewPlainResourceClientBuilder(
	build func(optionYaml []byte) (resourceClient ResourceClient, adaptor RequestAdapter, hooks []Hook, err error)) ResourceClientBuilder {
	return &plainResourceClientBuilder{build: build}
}

type plainDirector struct {
	direct func(url *url.URL, urlMeta *commonv1.UrlMeta) error
}

func (a *plainDirector) Direct(rawURL *url.URL, urlMeta *commonv1.UrlMeta) error {
	return a.direct(rawURL, urlMeta)
}

func NewPlainDirector(
	direct func(url *url.URL, urlMeta *commonv1.UrlMeta) error) Director {
	return &plainDirector{direct: direct}
}
