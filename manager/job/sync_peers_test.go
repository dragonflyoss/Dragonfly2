/*
 *     Copyright 2024 The Dragonfly Authors
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
	"sort"
	"testing"

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/pkg/idgen"
	"d7y.io/dragonfly/v2/pkg/types"
	resource "d7y.io/dragonfly/v2/scheduler/resource/standard"
	"github.com/stretchr/testify/assert"
)

func Test_diffPeers(t *testing.T) {
	type args struct {
		existingPeers []models.Peer
		currentPeers  []*resource.Host
	}
	tests := []struct {
		name         string
		args         args
		wantToUpsert []models.Peer
		wantToDelete []models.Peer
	}{
		{
			name: "append",
			args: args{
				existingPeers: []models.Peer{
					// delete for not existing
					generateModePeer("127.0.0.6", "foo6", 80, 80, types.HostTypeSuperSeed),
					// delete for original HostTypeNormal
					generateModePeer("127.0.0.5", "foo5", 80, 80, types.HostTypeNormal),
					// delete for type changed
					generateModePeer("127.0.0.4", "foo4", 80, 80, types.HostTypeNormal),
					// update for port changed
					generateModePeer("127.0.0.1", "foo1", 80, 443, types.HostTypeSuperSeed),
					// update for type changed
					generateModePeer("127.0.0.2", "foo2", 80, 80, types.HostTypeStrongSeed),
				},
				currentPeers: []*resource.Host{
					resource.NewHost(
						idgen.HostIDV2("127.0.0.1", "foo1", true),
						"127.0.0.1",
						"foo1",
						80,
						80,
						types.HostTypeSuperSeed),
					resource.NewHost(
						idgen.HostIDV2("127.0.0.2", "foo2", true),
						"127.0.0.2",
						"foo2",
						80,
						80,
						types.HostTypeSuperSeed),
					resource.NewHost(
						idgen.HostIDV2("127.0.0.3", "foo3", true),
						"127.0.0.3",
						"foo3",
						80,
						80,
						types.HostTypeSuperSeed), // append only
				},
			},
			wantToUpsert: []models.Peer{
				generateModePeer("127.0.0.1", "foo1", 80, 80, types.HostTypeSuperSeed),
				generateModePeer("127.0.0.2", "foo2", 80, 80, types.HostTypeSuperSeed),
				generateModePeer("127.0.0.3", "foo3", 80, 80, types.HostTypeSuperSeed),
			},
			wantToDelete: []models.Peer{
				generateModePeer("127.0.0.4", "foo4", 80, 80, types.HostTypeNormal),
				generateModePeer("127.0.0.5", "foo5", 80, 80, types.HostTypeNormal),
				generateModePeer("127.0.0.6", "foo6", 80, 80, types.HostTypeSuperSeed),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotToUpdate, gotToDelete := diffPeers(tt.args.existingPeers, tt.args.currentPeers)
			// sort the result to compare
			sort.Slice(gotToUpdate, func(i, j int) bool {
				return gotToUpdate[i].IP < gotToUpdate[j].IP
			})
			sort.Slice(gotToDelete, func(i, j int) bool {
				return gotToDelete[i].IP < gotToDelete[j].IP
			})
			assert.Equalf(t, tt.wantToUpsert, gotToUpdate, "diffPeers toUpsert(%v, %v)", tt.args.existingPeers, tt.args.currentPeers)
			assert.Equalf(t, tt.wantToDelete, gotToDelete, "diffPeers toDelete(%v, %v)", tt.args.existingPeers, tt.args.currentPeers)
		})
	}
}

func generateModePeer(ip, hostname string, port, downloadPort int32, typ types.HostType) models.Peer {
	return models.Peer{
		Hostname:     hostname,
		Type:         typ.Name(),
		IP:           ip,
		Port:         port,
		State:        models.PeerStateActive,
		DownloadPort: downloadPort,
	}
}
