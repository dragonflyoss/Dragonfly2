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
	"testing"

	"github.com/stretchr/testify/require"

	"d7y.io/dragonfly/v2/manager/types"
)

// func TestGetManifests is a test function for getManifests
func TestGetManifests(t *testing.T) {
	p := &preheat{}
	_, err := p.getManifests(context.Background(), &preheatImage{
		protocol: "https",
		domain:   "registry-1.docker.io",
		name:     "dragonflyoss/busybox",
		tag:      "1.35.0",
	}, "", "")

	require.NoError(t, err)
}

// func TestGetLayers is a test function for getLayers
func TestGetLayers(t *testing.T) {
	p := &preheat{}
	ps, err := p.getLayers(context.Background(), &preheatImage{
		protocol: "https",
		domain:   "registry-1.docker.io",
		name:     "dragonflyoss/busybox",
		tag:      "1.35.0",
	}, types.PreheatArgs{
		URL:  "https://registry-1.docker.io/v2/dragonflyoss/busybox/manifests/1.35.0",
		Type: "image",
	})

	require.NoError(t, err)
	require.NotEmpty(t, ps)
}
