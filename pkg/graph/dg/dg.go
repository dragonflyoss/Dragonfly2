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

//go:generate mockgen -destination mocks/dg_mock.go -source dg.go -package mocks

package dg

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

// DG is the interface used for directed graph.
type DG[T comparable] interface {
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

	// CanAddEdge indicates whether can add edge between two vertices.
	CanAddEdge(fromVertexID, toVertexID string) bool

	// DeleteVertexInEdges deletes inedges of vertex.
	DeleteVertexInEdges(id string) error

	// DeleteVertexOutEdges deletes outedges of vertex.
	DeleteVertexOutEdges(id string) error
}

// dg provides directed graph function.
type dg[T comparable] struct {
	vertices *sync.Map
	count    *atomic.Uint64
	mu       sync.RWMutex
}

// New returns a new DG interface.
func NewDG[T comparable]() DG[T] {
	return &dg[T]{
		vertices: &sync.Map{},
		count:    atomic.NewUint64(0),
		mu:       sync.RWMutex{},
	}
}

// AddVertex adds vertex to graph.
func (d *dg[T]) AddVertex(id string, value T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, loaded := d.vertices.LoadOrStore(id, NewVertex(id, value)); loaded {
		return ErrVertexAlreadyExists
	}

	d.count.Inc()
	return nil
}

// DeleteVertex deletes vertex graph.
func (d *dg[T]) DeleteVertex(id string) {
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
func (d *dg[T]) GetVertex(id string) (*Vertex[T], error) {
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
func (d *dg[T]) GetVertices() map[string]*Vertex[T] {
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
func (d *dg[T]) GetRandomVertices(n uint) []*Vertex[T] {
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
func (d *dg[T]) VertexCount() uint64 {
	return d.count.Load()
}

// AddEdge adds edge between two vertices.
func (d *dg[T]) AddEdge(fromVertexID, toVertexID string) error {
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

	if ok := fromVertex.Children.Add(toVertex); !ok {
		return ErrChildAlreadyExists
	}

	if ok := toVertex.Parents.Add(fromVertex); !ok {
		return ErrParnetAlreadyExists
	}

	return nil
}

// DeleteEdge deletes edge between two vertices.
func (d *dg[T]) DeleteEdge(fromVertexID, toVertexID string) error {
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

// CanAddEdge indicates whether can add edge between two vertices.
func (d *dg[T]) CanAddEdge(fromVertexID, toVertexID string) bool {
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

	return true
}

// DeleteVertexInEdges deletes inedges of vertex.
func (d *dg[T]) DeleteVertexInEdges(id string) error {
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
func (d *dg[T]) DeleteVertexOutEdges(id string) error {
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
func (d *dg[T]) GetSourceVertices() []*Vertex[T] {
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
func (d *dg[T]) GetSinkVertices() []*Vertex[T] {
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
