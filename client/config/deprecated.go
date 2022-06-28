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

package config

import (
	"errors"
	"fmt"
	"strings"
)

var DefaultSupernodesValue = &SupernodesValue{
	Nodes: []string{
		fmt.Sprintf("%s:%d", DefaultSchedulerIP, DefaultSchedulerPort),
	},
}

type SupernodesValue struct {
	Nodes []string
}

// String implements the pflag.Value interface.
func (sv *SupernodesValue) String() string {
	var result []string
	result = append(result, sv.Nodes...)
	return strings.Join(result, ",")
}

// Set implements the pflag.Value interface.
func (sv *SupernodesValue) Set(value string) error {
	nodes := strings.Split(value, ",")
	for _, n := range nodes {
		v := strings.Split(n, "=")
		if len(v) == 0 || len(v) > 2 {
			return errors.New("invalid nodes")
		}
		// ignore weight
		node := v[0]
		vv := strings.Split(node, ":")
		if len(vv) >= 2 {
			return errors.New("invalid nodes")
		}
		if len(vv) == 1 {
			node = fmt.Sprintf("%s:%d", node, DefaultSchedulerPort)
		}
		sv.Nodes = append(sv.Nodes, node)
	}
	return nil
}

// Type implements the pflag.Value interface.
func (sv *SupernodesValue) Type() string {
	return "supernodes"
}
