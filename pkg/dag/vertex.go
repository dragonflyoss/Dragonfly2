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

package dag

import "d7y.io/dragonfly/v2/pkg/container/set"

// Vertex is a vertex of the directed acyclic graph.
type Vertex struct {
	ID       string
	Value    any
	Parents  set.SafeSet
	Children set.SafeSet
}

// New returns a new Vertex instance.
func NewVertex(id string, value any) *Vertex {
	return &Vertex{
		ID:       id,
		Value:    value,
		Parents:  set.NewSafeSet(),
		Children: set.NewSafeSet(),
	}
}

// Degree returns the degree of vertex.
func (v *Vertex) Degree() int {
	return int(v.Parents.Len() + v.Children.Len())
}

// InDegree returns the indegree of vertex.
func (v *Vertex) InDegree() int {
	return int(v.Parents.Len())
}

// OutDegree returns the outdegree of vertex.
func (v *Vertex) OutDegree() int {
	return int(v.Children.Len())
}

// DeleteInEdges deletes inedges of vertex.
func (v *Vertex) DeleteInEdges() {
	for _, value := range v.Parents.Values() {
		vertex, ok := value.(*Vertex)
		if !ok {
			continue
		}

		vertex.Children.Delete(v)
	}

	v.Parents = set.NewSafeSet()
}

// DeleteOutEdges deletes outedges of vertex.
func (v *Vertex) DeleteOutEdges() {
	for _, value := range v.Children.Values() {
		vertex, ok := value.(*Vertex)
		if !ok {
			continue
		}

		vertex.Parents.Delete(v)
	}

	v.Children = set.NewSafeSet()
}
