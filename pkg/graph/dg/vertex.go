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

package dg

import (
	"d7y.io/dragonfly/v2/pkg/container/set"
)

// Vertex is a vertex of the directed graph.
type Vertex[T comparable] struct {
	ID       string
	Value    T
	Parents  set.Set[*Vertex[T]]
	Children set.Set[*Vertex[T]]
}

// New returns a new Vertex instance.
func NewVertex[T comparable](id string, value T) *Vertex[T] {
	return &Vertex[T]{
		ID:       id,
		Value:    value,
		Parents:  set.New[*Vertex[T]](),
		Children: set.New[*Vertex[T]](),
	}
}

// Degree returns the degree of vertex.
func (v *Vertex[T]) Degree() int {
	return int(v.Parents.Len() + v.Children.Len())
}

// InDegree returns the indegree of vertex.
func (v *Vertex[T]) InDegree() int {
	return int(v.Parents.Len())
}

// OutDegree returns the outdegree of vertex.
func (v *Vertex[T]) OutDegree() int {
	return int(v.Children.Len())
}
