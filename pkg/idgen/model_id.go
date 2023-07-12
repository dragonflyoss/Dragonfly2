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

package idgen

import (
	"d7y.io/dragonfly/v2/pkg/digest"
)

const (
	// GNNModelNameSuffix is suffix of GNN model id.
	GNNModelNameSuffix = "gnn"

	// MLPModelNameSuffix is suffix of MLP model id.
	MLPModelNameSuffix = "mlp"
)

// GNNModelIDV1 generates v1 version of gnn model id.
func GNNModelIDV1(ip, hostname string) string {
	return digest.SHA256FromStrings(ip, hostname, GNNModelNameSuffix)
}

// MLPModelIDV1 generates v1 version of mlp model id.
func MLPModelIDV1(ip, hostname string) string {
	return digest.SHA256FromStrings(ip, hostname, MLPModelNameSuffix)
}
