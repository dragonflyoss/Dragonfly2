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

package service

import (
	"d7y.io/dragonfly/v2/trainer/config"
	"d7y.io/dragonfly/v2/trainer/storage"
)

// V1 is the interface for v1 version of the service.
type V1 struct {
	// Trainer service config.
	config *config.Config

	// Storage Interface.
	storage storage.Storage
}

// New v1 version of service instance.
func NewV1(
	cfg *config.Config,
	storage storage.Storage,

) *V1 {
	return &V1{
		config:  cfg,
		storage: storage,
	}
}
