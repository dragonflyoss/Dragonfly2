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

import "d7y.io/dragonfly/v2/pkg/container/set"

// Vertex is a vertex of the directed graph.
type Vertex[T comparable] struct {
	ID       string
	Value    T
	Parents  set.SafeSet[*Vertex[T]]
	Children set.SafeSet[*Vertex[T]]
}

// New returns a new Vertex instance.
func NewVertex[T comparable](id string, value T) *Vertex[T] {
	return &Vertex[T]{
		ID:       id,
		Value:    value,
		Parents:  set.NewSafeSet[*Vertex[T]](),
		Children: set.NewSafeSet[*Vertex[T]](),
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

// DeleteInEdges deletes inedges of vertex.
func (v *Vertex[T]) DeleteInEdges() {
	for _, parent := range v.Parents.Values() {
		parent.Children.Delete(v)
	}

	v.Parents = set.NewSafeSet[*Vertex[T]]()
}

// DeleteOutEdges deletes outedges of vertex.
func (v *Vertex[T]) DeleteOutEdges() {
	for _, child := range v.Children.Values() {
		child.Parents.Delete(v)
	}

	v.Children = set.NewSafeSet[*Vertex[T]]()
}
