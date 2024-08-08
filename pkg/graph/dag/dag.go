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

//go:generate mockgen -destination mocks/dag_mock.go -source dag.go -package mocks

package dag

import (
	"errors"
	"sync"

	"go.uber.org/atomic"

	"d7y.io/dragonfly/v2/pkg/container/set"
)

var (
	// ErrVertexNotFound represents vertex not found.
	ErrVertexNotFound = errors.New("vertex not found")

	// ErrVertexInvalid represents vertex invalid.
	ErrVertexInvalid = errors.New("vertex invalid")

	// ErrVertexAlreadyExists represents vertex already exists.
	ErrVertexAlreadyExists = errors.New("vertex already exists")

	// ErrParnetAlreadyExists represents parent of vertex already exists.
	ErrParnetAlreadyExists = errors.New("parent of vertex already exists")

	// ErrChildAlreadyExists represents child of vertex already exists.
	ErrChildAlreadyExists = errors.New("child of vertex already exists")

	// ErrCycleBetweenVertices represents cycle between vertices.
	ErrCycleBetweenVertices = errors.New("cycle between vertices")
)

// DAG is the interface used for directed acyclic graph.
type DAG[T comparable] interface {
	// AddVertex adds vertex to graph.
	AddVertex(id string, value T) error

	// DeleteVertex deletes vertex graph.
	DeleteVertex(id string)

	// GetVertex gets vertex from graph.
	GetVertex(id string) (*Vertex[T], error)

	// GetVertices returns map of vertices.
	GetVertices() map[string]*Vertex[T]

	// GetRandomVertices returns random map of vertices.
	GetRandomVertices(n uint) []*Vertex[T]

	// GetSourceVertices returns source vertices.
	GetSourceVertices() []*Vertex[T]

	// GetSinkVertices returns sink vertices.
	GetSinkVertices() []*Vertex[T]

	// VertexCount returns count of vertices.
	VertexCount() uint64

	// AddEdge adds edge between two vertices.
	AddEdge(fromVertexID, toVertexID string) error

	// DeleteEdge deletes edge between two vertices.
	DeleteEdge(fromVertexID, toVertexID string) error

	// CanAddEdge finds whether there are circles through depth-first search.
	CanAddEdge(fromVertexID, toVertexID string) bool

	// DeleteVertexInEdges deletes inedges of vertex.
	DeleteVertexInEdges(id string) error

	// DeleteVertexOutEdges deletes outedges of vertex.
	DeleteVertexOutEdges(id string) error
}

// dag provides directed acyclic graph function.
type dag[T comparable] struct {
	vertices *sync.Map
	count    *atomic.Uint64
	mu       sync.RWMutex
}

// New returns a new DAG interface.
func NewDAG[T comparable]() DAG[T] {
	return &dag[T]{
		vertices: &sync.Map{},
		count:    atomic.NewUint64(0),
		mu:       sync.RWMutex{},
	}
}

// AddVertex adds vertex to graph.
func (d *dag[T]) AddVertex(id string, value T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, loaded := d.vertices.LoadOrStore(id, NewVertex(id, value)); loaded {
		return ErrVertexAlreadyExists
	}

	d.count.Inc()
	return nil
}

// DeleteVertex deletes vertex graph.
func (d *dag[T]) DeleteVertex(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rawVertex, loaded := d.vertices.Load(id)
	if !loaded {
		return
	}

	vertex, ok := rawVertex.(*Vertex[T])
	if !ok {
		return
	}

	for _, parent := range vertex.Parents.Values() {
		parent.Children.Delete(vertex)
	}

	for _, child := range vertex.Children.Values() {
		child.Parents.Delete(vertex)
		continue
	}

	d.vertices.Delete(id)
	d.count.Dec()
}

// GetVertex gets vertex from graph.
func (d *dag[T]) GetVertex(id string) (*Vertex[T], error) {
	rawVertex, loaded := d.vertices.Load(id)
	if !loaded {
		return nil, ErrVertexNotFound
	}

	vertex, ok := rawVertex.(*Vertex[T])
	if !ok {
		return nil, ErrVertexInvalid
	}

	return vertex, nil
}

// GetVertices returns map of vertices.
func (d *dag[T]) GetVertices() map[string]*Vertex[T] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	vertices := make(map[string]*Vertex[T], d.count.Load())
	d.vertices.Range(func(key, value interface{}) bool {
		vertex, ok := value.(*Vertex[T])
		if !ok {
			return true
		}

		id, ok := key.(string)
		if !ok {
			return true
		}

		vertices[id] = vertex
		return true
	})

	return vertices
}

// GetRandomVertices returns random map of vertices.
func (d *dag[T]) GetRandomVertices(n uint) []*Vertex[T] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if n == 0 {
		return nil
	}

	randomVertices := make([]*Vertex[T], 0, n)
	d.vertices.Range(func(key, value interface{}) bool {
		vertex, ok := value.(*Vertex[T])
		if !ok {
			return true
		}

		randomVertices = append(randomVertices, vertex)
		return uint(len(randomVertices)) < n
	})

	return randomVertices
}

// VertexCount returns count of vertices.
func (d *dag[T]) VertexCount() uint64 {
	return d.count.Load()
}

// AddEdge adds edge between two vertices.
func (d *dag[T]) AddEdge(fromVertexID, toVertexID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if fromVertexID == toVertexID {
		return ErrCycleBetweenVertices
	}

	fromVertex, err := d.GetVertex(fromVertexID)
	if err != nil {
		return err
	}

	toVertex, err := d.GetVertex(toVertexID)
	if err != nil {
		return err
	}

	for _, child := range fromVertex.Children.Values() {
		if child.ID == toVertexID {
			return ErrCycleBetweenVertices
		}
	}

	if d.depthFirstSearch(toVertexID, fromVertexID) {
		return ErrCycleBetweenVertices
	}

	if ok := fromVertex.Children.Add(toVertex); !ok {
		return ErrChildAlreadyExists
	}

	if ok := toVertex.Parents.Add(fromVertex); !ok {
		return ErrParnetAlreadyExists
	}

	return nil
}

// DeleteEdge deletes edge between two vertices.
func (d *dag[T]) DeleteEdge(fromVertexID, toVertexID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	fromVertex, err := d.GetVertex(fromVertexID)
	if err != nil {
		return err
	}

	toVertex, err := d.GetVertex(toVertexID)
	if err != nil {
		return err
	}

	fromVertex.Children.Delete(toVertex)
	toVertex.Parents.Delete(fromVertex)
	return nil
}

// CanAddEdge finds whether there are circles through depth-first search.
func (d *dag[T]) CanAddEdge(fromVertexID, toVertexID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if fromVertexID == toVertexID {
		return false
	}

	fromVertex, err := d.GetVertex(fromVertexID)
	if err != nil {
		return false
	}

	if _, err := d.GetVertex(toVertexID); err != nil {
		return false
	}

	for _, child := range fromVertex.Children.Values() {
		if child.ID == toVertexID {
			return false
		}
	}

	if d.depthFirstSearch(toVertexID, fromVertexID) {
		return false
	}

	return true
}

// DeleteVertexInEdges deletes inedges of vertex.
func (d *dag[T]) DeleteVertexInEdges(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	vertex, err := d.GetVertex(id)
	if err != nil {
		return err
	}

	for _, parent := range vertex.Parents.Values() {
		parent.Children.Delete(vertex)
	}

	vertex.Parents = set.NewSafeSet[*Vertex[T]]()
	return nil
}

// DeleteVertexOutEdges deletes outedges of vertex.
func (d *dag[T]) DeleteVertexOutEdges(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	vertex, err := d.GetVertex(id)
	if err != nil {
		return err
	}

	for _, child := range vertex.Children.Values() {
		child.Parents.Delete(vertex)
	}

	vertex.Children = set.NewSafeSet[*Vertex[T]]()
	return nil
}

// GetSourceVertices returns source vertices.
func (d *dag[T]) GetSourceVertices() []*Vertex[T] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var sourceVertices []*Vertex[T]
	for _, vertex := range d.GetVertices() {
		if vertex.InDegree() == 0 {
			sourceVertices = append(sourceVertices, vertex)
		}
	}

	return sourceVertices
}

// GetSinkVertices returns sink vertices.
func (d *dag[T]) GetSinkVertices() []*Vertex[T] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var sinkVertices []*Vertex[T]
	for _, vertex := range d.GetVertices() {
		if vertex.OutDegree() == 0 {
			sinkVertices = append(sinkVertices, vertex)
		}
	}

	return sinkVertices
}

// depthFirstSearch is a depth-first search of the directed acyclic graph.
func (d *dag[T]) depthFirstSearch(fromVertexID, toVertexID string) bool {
	successors := make(map[string]struct{})
	d.search(fromVertexID, successors)
	_, ok := successors[toVertexID]
	return ok
}

// search finds successors of vertex.
func (d *dag[T]) search(vertexID string, successors map[string]struct{}) {
	vertex, err := d.GetVertex(vertexID)
	if err != nil {
		return
	}

	for _, child := range vertex.Children.Values() {
		if _, ok := successors[child.ID]; !ok {
			successors[child.ID] = struct{}{}
			d.search(child.ID, successors)
		}
	}
}
