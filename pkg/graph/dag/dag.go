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
	"math/rand"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
)

var (
	// ErrVertexNotFound represents vertex not found.
	ErrVertexNotFound = errors.New("vertex not found")

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

	// GetVertexKeys returns keys of vertices.
	GetVertexKeys() []string

	// GetSourceVertices returns source vertices.
	GetSourceVertices() []*Vertex[T]

	// GetSinkVertices returns sink vertices.
	GetSinkVertices() []*Vertex[T]

	// VertexCount returns count of vertices.
	VertexCount() int

	// AddEdge adds edge between two vertices.
	AddEdge(fromVertexID, toVertexID string) error

	// DeleteEdge deletes edge between two vertices.
	DeleteEdge(fromVertexID, toVertexID string) error

	// CanAddEdge finds whether there are circles through depth-first search.
	CanAddEdge(fromVertexID, toVertexID string) bool
}

// dag provides directed acyclic graph function.
type dag[T comparable] struct {
	mu       sync.RWMutex
	vertices cmap.ConcurrentMap[string, *Vertex[T]]
}

// New returns a new DAG interface.
func NewDAG[T comparable]() DAG[T] {
	return &dag[T]{
		vertices: cmap.New[*Vertex[T]](),
	}
}

// AddVertex adds vertex to graph.
func (d *dag[T]) AddVertex(id string, value T) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.vertices.Get(id); ok {
		return ErrVertexAlreadyExists
	}

	d.vertices.Set(id, NewVertex(id, value))
	return nil
}

// DeleteVertex deletes vertex graph.
func (d *dag[T]) DeleteVertex(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	vertex, ok := d.vertices.Get(id)
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

	d.vertices.Remove(id)
}

// GetVertex gets vertex from graph.
func (d *dag[T]) GetVertex(id string) (*Vertex[T], error) {
	vertex, ok := d.vertices.Get(id)
	if !ok {
		return nil, ErrVertexNotFound
	}

	return vertex, nil
}

// GetVertices returns map of vertices.
func (d *dag[T]) GetVertices() map[string]*Vertex[T] {
	return d.vertices.Items()
}

// GetRandomVertices returns random map of vertices.
func (d *dag[T]) GetRandomVertices(n uint) []*Vertex[T] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	keys := d.GetVertexKeys()
	if int(n) >= len(keys) {
		n = uint(len(keys))
	}

	rand.Seed(time.Now().Unix())
	permutation := rand.Perm(len(keys))[:n]
	randomVertices := make([]*Vertex[T], 0, n)
	for _, v := range permutation {
		key := keys[v]
		if vertex, err := d.GetVertex(key); err == nil {
			randomVertices = append(randomVertices, vertex)
		}
	}

	return randomVertices
}

// GetVertexKeys returns keys of vertices.
func (d *dag[T]) GetVertexKeys() []string {
	return d.vertices.Keys()
}

// VertexCount returns count of vertices.
func (d *dag[T]) VertexCount() int {
	return d.vertices.Count()
}

// CanAddEdge finds whether there are circles through depth-first search.
func (d *dag[T]) CanAddEdge(fromVertexID, toVertexID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if fromVertexID == toVertexID {
		return false
	}

	fromVertex, ok := d.vertices.Get(fromVertexID)
	if !ok {
		return false
	}

	if _, ok := d.vertices.Get(toVertexID); !ok {
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

// AddEdge adds edge between two vertices.
func (d *dag[T]) AddEdge(fromVertexID, toVertexID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if fromVertexID == toVertexID {
		return ErrCycleBetweenVertices
	}

	fromVertex, ok := d.vertices.Get(fromVertexID)
	if !ok {
		return ErrVertexNotFound
	}

	toVertex, ok := d.vertices.Get(toVertexID)
	if !ok {
		return ErrVertexNotFound
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

	fromVertex, ok := d.vertices.Get(fromVertexID)
	if !ok {
		return ErrVertexNotFound
	}

	toVertex, ok := d.vertices.Get(toVertexID)
	if !ok {
		return ErrVertexNotFound
	}

	fromVertex.Children.Delete(toVertex)
	toVertex.Parents.Delete(fromVertex)
	return nil
}

// GetSourceVertices returns source vertices.
func (d *dag[T]) GetSourceVertices() []*Vertex[T] {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var sourceVertices []*Vertex[T]
	for _, vertex := range d.vertices.Items() {
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
	for _, vertex := range d.vertices.Items() {
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
	vertex, ok := d.vertices.Get(vertexID)
	if !ok {
		return
	}

	for _, child := range vertex.Children.Values() {
		if _, ok := successors[child.ID]; !ok {
			successors[child.ID] = struct{}{}
			d.search(child.ID, successors)
		}
	}
}
