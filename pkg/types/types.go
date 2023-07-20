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

package types

import (
	"encoding/json"
	"errors"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"
	commonv2 "d7y.io/api/v2/pkg/apis/common/v2"
)

// PEMContent supports load PEM format from file or just inline PEM format content
type PEMContent string

func (p *PEMContent) UnmarshalJSON(b []byte) error {
	var s string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return err
	}

	return p.loadPEM(s)
}

func (p *PEMContent) UnmarshalYAML(node *yaml.Node) error {
	var s string
	switch node.Kind {
	case yaml.ScalarNode:
		if err := node.Decode(&s); err != nil {
			return err
		}
	default:
		return errors.New("invalid pem content")
	}

	return p.loadPEM(s)
}

func (p *PEMContent) loadPEM(content string) error {
	if content == "" {
		*p = PEMContent("")
		return nil
	}

	// inline PEM, just return
	if strings.HasPrefix(strings.TrimSpace(content), "-----BEGIN ") {
		val := strings.TrimSpace(content)
		*p = PEMContent(val)
		return nil
	}

	file, err := os.ReadFile(content)
	if err != nil {
		return err
	}
	val := strings.TrimSpace(string(file))
	*p = PEMContent(val)
	return nil
}

// HostType is the type of host.
type HostType int

const (
	// HostTypeNormal is the normal type of host.
	HostTypeNormal HostType = iota

	// HostTypeSuperSeed is the super seed type of host.
	HostTypeSuperSeed

	// HostTypeStrongSeed is the strong seed type of host.
	HostTypeStrongSeed

	// HostTypeWeakSeed is the weak seed type of host.
	HostTypeWeakSeed
)

const (
	// HostTypeNormalName is the name of normal host type.
	HostTypeNormalName = "normal"

	// HostTypeSuperSeedName is the name of super host type.
	HostTypeSuperSeedName = "super"

	// HostTypeStrongSeedName is the name of strong host type.
	HostTypeStrongSeedName = "strong"

	// HostTypeWeakSeedName is the name of weak host type.
	HostTypeWeakSeedName = "weak"
)

// Name returns the name of host type.
func (h HostType) Name() string {
	switch h {
	case HostTypeSuperSeed:
		return HostTypeSuperSeedName
	case HostTypeStrongSeed:
		return HostTypeStrongSeedName
	case HostTypeWeakSeed:
		return HostTypeWeakSeedName
	}

	return HostTypeNormalName
}

// ParseHostType parses host type by name.
func ParseHostType(name string) HostType {
	switch name {
	case HostTypeSuperSeedName:
		return HostTypeSuperSeed
	case HostTypeStrongSeedName:
		return HostTypeStrongSeed
	case HostTypeWeakSeedName:
		return HostTypeWeakSeed
	}

	return HostTypeNormal
}

// TaskTypeV1ToV2 converts task type from v1 to v2.
func TaskTypeV1ToV2(typ commonv1.TaskType) commonv2.TaskType {
	switch typ {
	case commonv1.TaskType_Normal:
		return commonv2.TaskType_DFDAEMON
	case commonv1.TaskType_DfCache:
		return commonv2.TaskType_DFCACHE
	case commonv1.TaskType_DfStore:
		return commonv2.TaskType_DFSTORE
	}

	return commonv2.TaskType_DFDAEMON
}

// TaskTypeV2ToV1 converts task type from v2 to v1.
func TaskTypeV2ToV1(typ commonv2.TaskType) commonv1.TaskType {
	switch typ {
	case commonv2.TaskType_DFDAEMON:
		return commonv1.TaskType_Normal
	case commonv2.TaskType_DFCACHE:
		return commonv1.TaskType_DfCache
	case commonv2.TaskType_DFSTORE:
		return commonv1.TaskType_DfStore
	}

	return commonv1.TaskType_Normal
}

// PriorityV1ToV2 converts priority from v1 to v2.
func PriorityV1ToV2(priority commonv1.Priority) commonv2.Priority {
	switch priority {
	case commonv1.Priority_LEVEL0:
		return commonv2.Priority_LEVEL0
	case commonv1.Priority_LEVEL1:
		return commonv2.Priority_LEVEL1
	case commonv1.Priority_LEVEL2:
		return commonv2.Priority_LEVEL2
	case commonv1.Priority_LEVEL3:
		return commonv2.Priority_LEVEL3
	case commonv1.Priority_LEVEL4:
		return commonv2.Priority_LEVEL4
	case commonv1.Priority_LEVEL5:
		return commonv2.Priority_LEVEL5
	case commonv1.Priority_LEVEL6:
		return commonv2.Priority_LEVEL6
	}

	return commonv2.Priority_LEVEL0
}

// PriorityV2ToV1 converts priority from v2 to v1.
func PriorityV2ToV1(priority commonv2.Priority) commonv1.Priority {
	switch priority {
	case commonv2.Priority_LEVEL0:
		return commonv1.Priority_LEVEL0
	case commonv2.Priority_LEVEL1:
		return commonv1.Priority_LEVEL1
	case commonv2.Priority_LEVEL2:
		return commonv1.Priority_LEVEL2
	case commonv2.Priority_LEVEL3:
		return commonv1.Priority_LEVEL3
	case commonv2.Priority_LEVEL4:
		return commonv1.Priority_LEVEL4
	case commonv2.Priority_LEVEL5:
		return commonv1.Priority_LEVEL5
	case commonv2.Priority_LEVEL6:
		return commonv1.Priority_LEVEL6
	}

	return commonv1.Priority_LEVEL0
}

// SizeScopeV1ToV2 converts size scope from v1 to v2.
func SizeScopeV1ToV2(sizeScope commonv1.SizeScope) commonv2.SizeScope {
	switch sizeScope {
	case commonv1.SizeScope_NORMAL:
		return commonv2.SizeScope_NORMAL
	case commonv1.SizeScope_SMALL:
		return commonv2.SizeScope_SMALL
	case commonv1.SizeScope_TINY:
		return commonv2.SizeScope_TINY
	case commonv1.SizeScope_EMPTY:
		return commonv2.SizeScope_EMPTY
	case commonv1.SizeScope_UNKNOW:
		return commonv2.SizeScope_UNKNOW
	}

	return commonv2.SizeScope_UNKNOW
}

// SizeScopeV2ToV1 converts size scope from v2 to v1.
func SizeScopeV2ToV1(sizeScope commonv2.SizeScope) commonv1.SizeScope {
	switch sizeScope {
	case commonv2.SizeScope_NORMAL:
		return commonv1.SizeScope_NORMAL
	case commonv2.SizeScope_SMALL:
		return commonv1.SizeScope_SMALL
	case commonv2.SizeScope_TINY:
		return commonv1.SizeScope_TINY
	case commonv2.SizeScope_EMPTY:
		return commonv1.SizeScope_EMPTY
	case commonv2.SizeScope_UNKNOW:
		return commonv1.SizeScope_UNKNOW
	}

	return commonv1.SizeScope_UNKNOW
}
