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

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDAG(t *testing.T) {
	d := NewDAG[string]()
	assert := assert.New(t)
	assert.Equal(reflect.TypeOf(d).Elem().Name(), "dag[string]")
}

func TestDAGAddVertex(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		value  any
		expect func(t *testing.T, d DAG[string], err error)
	}{
		{
			name:  "add vertex",
			id:    mockVertexID,
			value: mockVertexValue,
			expect: func(t *testing.T, d DAG[string], err error) {
				assert := assert.New(t)
				assert.NoError(err)
			},
		},
		{
			name:  "vertex already exists",
			id:    mockVertexID,
			value: mockVertexValue,
			expect: func(t *testing.T, d DAG[string], err error) {
				assert := assert.New(t)
				assert.NoError(err)

				assert.EqualError(d.AddVertex(mockVertexID, mockVertexValue), ErrVertexAlreadyExists.Error())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d, d.AddVertex(tc.id, tc.name))
		})
	}
}

func TestDAGDeleteVertex(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "delete vertex",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				if err := d.AddVertex(mockVertexID, mockVertexValue); err != nil {
					assert.NoError(err)
				}
				d.DeleteVertex(mockVertexID)

				_, err := d.GetVertex(mockVertexID)
				assert.EqualError(err, ErrVertexNotFound.Error())
			},
		},
		{
			name: "delete vertex with edges",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)

				var (
					mockToVertexID = "baz"
				)
				if err := d.AddVertex(mockToVertexID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexID, mockToVertexID); err != nil {
					assert.NoError(err)
				}

				d.DeleteVertex(mockVertexID)

				_, err := d.GetVertex(mockVertexID)
				assert.EqualError(err, ErrVertexNotFound.Error())

				vertex, err := d.GetVertex(mockToVertexID)
				assert.NoError(err)
				assert.Equal(vertex.Parents.Len(), uint(0))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGGetVertex(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "get vertex",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				if err := d.AddVertex(mockVertexID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				vertex, err := d.GetVertex(mockVertexID)
				assert.NoError(err)
				assert.Equal(vertex.ID, mockVertexID)
				assert.Equal(vertex.Value, mockVertexValue)
				assert.Equal(vertex.Parents.Len(), uint(0))
				assert.Equal(vertex.Children.Len(), uint(0))
			},
		},
		{
			name: "vertex not found",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				_, err := d.GetVertex(mockVertexID)
				assert.EqualError(err, ErrVertexNotFound.Error())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGVertexVertexCount(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "get length of vertex",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				if err := d.AddVertex(mockVertexID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				d.VertexCount()
				assert.Equal(d.VertexCount(), 1)

				d.DeleteVertex(mockVertexID)
				assert.Equal(d.VertexCount(), 0)
			},
		},
		{
			name: "empty dag",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				assert.Equal(d.VertexCount(), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGGetVertices(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "get vertices",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				if err := d.AddVertex(mockVertexID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				vertices := d.GetVertices()
				assert.Equal(len(vertices), 1)
				assert.Equal(vertices[mockVertexID].ID, mockVertexID)
				assert.Equal(vertices[mockVertexID].Value, mockVertexValue)

				d.DeleteVertex(mockVertexID)
				vertices = d.GetVertices()
				assert.Equal(len(vertices), 0)
			},
		},
		{
			name: "dag is empty",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				vertices := d.GetVertices()
				assert.Equal(len(vertices), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGGetRandomVertices(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "get random vertices",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				vertices := d.GetRandomVertices(0)
				assert.Equal(len(vertices), 0)

				vertices = d.GetRandomVertices(1)
				assert.Equal(len(vertices), 1)

				vertices = d.GetRandomVertices(2)
				assert.Equal(len(vertices), 2)

				vertices = d.GetRandomVertices(3)
				assert.Equal(len(vertices), 2)
			},
		},
		{
			name: "dag is empty",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				vertices := d.GetRandomVertices(0)
				assert.Equal(len(vertices), 0)

				vertices = d.GetRandomVertices(1)
				assert.Equal(len(vertices), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGGetVertexKeys(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "get keys of vertices",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				if err := d.AddVertex(mockVertexID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				keys := d.GetVertexKeys()
				assert.Equal(len(keys), 1)
				assert.Equal(keys[0], mockVertexID)

				d.DeleteVertex(mockVertexID)
				keys = d.GetVertexKeys()
				assert.Equal(len(keys), 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGAddEdge(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "add edge",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
					mockVertexGID = "bag"
					mockVertexHID = "bah"
					mockVertexIID = "bai"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexGID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexHID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexIID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexFID, mockVertexGID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexFID, mockVertexHID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexGID, mockVertexIID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexIID, mockVertexHID); err != nil {
					assert.NoError(err)
				}
			},
		},
		{
			name: "cycle between vertices",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
					mockVertexGID = "bag"
					mockVertexHID = "bah"
					mockVertexIID = "bai"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexGID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexHID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexIID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrCycleBetweenVertices.Error())
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexFID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrCycleBetweenVertices.Error())
				}

				if err := d.AddEdge(mockVertexFID, mockVertexGID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexGID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrCycleBetweenVertices.Error())
				}

				if err := d.AddEdge(mockVertexGID, mockVertexHID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexHID, mockVertexIID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexIID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrCycleBetweenVertices.Error())
				}
			},
		},
		{
			name: "vertex not found",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.EqualError(err, ErrVertexNotFound.Error())
				}

				if err := d.AddEdge(mockVertexFID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrVertexNotFound.Error())
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGCanAddEdge(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "can add edge",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
					mockVertexGID = "bag"
					mockVertexHID = "bah"
					mockVertexIID = "bai"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexGID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexHID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexIID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexFID, mockVertexGID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexFID, mockVertexHID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexGID, mockVertexIID); err != nil {
					assert.NoError(err)
				}

				ok := d.CanAddEdge(mockVertexIID, mockVertexHID)
				assert.True(ok)
			},
		},
		{
			name: "cycle between vertices",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
					mockVertexGID = "bag"
					mockVertexHID = "bah"
					mockVertexIID = "bai"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexGID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexHID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexIID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrCycleBetweenVertices.Error())
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexFID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrCycleBetweenVertices.Error())
				}

				if err := d.AddEdge(mockVertexFID, mockVertexGID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexGID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrCycleBetweenVertices.Error())
				}

				if err := d.AddEdge(mockVertexGID, mockVertexHID); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexHID, mockVertexIID); err != nil {
					assert.NoError(err)
				}

				ok := d.CanAddEdge(mockVertexIID, mockVertexEID)
				assert.False(ok)
			},
		},
		{
			name: "vertex not found",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				ok := d.CanAddEdge(mockVertexEID, mockVertexFID)
				assert.False(ok)
				ok = d.CanAddEdge(mockVertexFID, mockVertexEID)
				assert.False(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGDeleteEdge(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "delete edge",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				if err := d.DeleteEdge(mockVertexFID, mockVertexEID); err != nil {
					assert.NoError(err)
				}

				vf, err := d.GetVertex(mockVertexFID)
				assert.NoError(err)
				assert.Equal(vf.Parents.Len(), uint(1))
				ve, err := d.GetVertex(mockVertexEID)
				assert.NoError(err)
				assert.Equal(ve.Children.Len(), uint(1))

				if err := d.DeleteEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				vf, err = d.GetVertex(mockVertexFID)
				assert.NoError(err)
				assert.Equal(vf.Parents.Len(), uint(0))
				ve, err = d.GetVertex(mockVertexEID)
				assert.NoError(err)
				assert.Equal(ve.Children.Len(), uint(0))
			},
		},
		{
			name: "vertex not found",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
				)

				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.EqualError(err, ErrVertexNotFound.Error())
				}

				if err := d.AddEdge(mockVertexFID, mockVertexEID); err != nil {
					assert.EqualError(err, ErrVertexNotFound.Error())
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGSourceVertices(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "get source vertices",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
				)
				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				sourceVertices := d.GetSourceVertices()
				assert.Equal(len(sourceVertices), 1)
				assert.Equal(sourceVertices[0].Value, mockVertexValue)
			},
		},
		{
			name: "source vertices not found",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				sourceVertices := d.GetSourceVertices()
				assert.Equal(len(sourceVertices), 0)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func TestDAGSinkVertices(t *testing.T) {
	tests := []struct {
		name   string
		expect func(t *testing.T, d DAG[string])
	}{
		{
			name: "get sink vertices",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				var (
					mockVertexEID = "bae"
					mockVertexFID = "baf"
				)
				if err := d.AddVertex(mockVertexEID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddVertex(mockVertexFID, mockVertexValue); err != nil {
					assert.NoError(err)
				}

				if err := d.AddEdge(mockVertexEID, mockVertexFID); err != nil {
					assert.NoError(err)
				}

				sinkVertices := d.GetSinkVertices()
				assert.Equal(len(sinkVertices), 1)
				assert.Equal(sinkVertices[0].Value, mockVertexValue)
			},
		},
		{
			name: "sink vertices not found",
			expect: func(t *testing.T, d DAG[string]) {
				assert := assert.New(t)
				sinkVertices := d.GetSinkVertices()
				assert.Equal(len(sinkVertices), 0)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDAG[string]()
			tc.expect(t, d)
		})
	}
}

func BenchmarkDAGAddVertex(b *testing.B) {
	var ids []string
	d := NewDAG[string]()
	for n := 0; n < b.N; n++ {
		ids = append(ids, fmt.Sprint(n))
	}

	b.ResetTimer()
	for _, id := range ids {
		if err := d.AddVertex(id, string(id)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDAGDeleteVertex(b *testing.B) {
	var ids []string
	d := NewDAG[string]()
	for n := 0; n < b.N; n++ {
		id := fmt.Sprint(n)
		if err := d.AddVertex(id, string(id)); err != nil {
			b.Fatal(err)
		}

		ids = append(ids, id)
	}

	b.ResetTimer()
	for _, id := range ids {
		d.DeleteVertex(id)
	}
}

func BenchmarkDAGGetRandomKeys(b *testing.B) {
	d := NewDAG[string]()
	for n := 0; n < b.N; n++ {
		id := fmt.Sprint(n)
		if err := d.AddVertex(id, string(id)); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		vertices := d.GetRandomVertices(uint(n))
		if len(vertices) != n {
			b.Fatal(errors.New("get random vertices failed"))
		}
	}
}

func BenchmarkDAGDeleteVertexWithMultiEdges(b *testing.B) {
	var ids []string
	d := NewDAG[string]()
	for n := 0; n < b.N; n++ {
		id := fmt.Sprint(n)
		if err := d.AddVertex(id, string(id)); err != nil {
			b.Fatal(err)
		}

		ids = append(ids, id)
	}

	edgeCount := 5
	for index, id := range ids {
		if index+edgeCount > len(ids)-1 {
			break
		}

		for n := 1; n < edgeCount; n++ {
			if err := d.AddEdge(id, ids[index+n]); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for _, id := range ids {
		d.DeleteVertex(id)
	}
}

func BenchmarkDAGAddEdge(b *testing.B) {
	var ids []string
	d := NewDAG[string]()
	for n := 0; n < b.N; n++ {
		id := fmt.Sprint(n)
		if err := d.AddVertex(id, string(id)); err != nil {
			b.Fatal(err)
		}

		ids = append(ids, id)
	}

	b.ResetTimer()
	for index, id := range ids {
		if index < 1 {
			continue
		}

		if err := d.AddEdge(id, ids[index-1]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDAGAddEdgeWithMultiEdges(b *testing.B) {
	var ids []string
	d := NewDAG[string]()
	for n := 0; n < b.N; n++ {
		id := fmt.Sprint(n)
		if err := d.AddVertex(id, string(id)); err != nil {
			b.Fatal(err)
		}

		ids = append(ids, id)
	}

	edgeCount := 5
	for index, id := range ids {
		if index+edgeCount > len(ids)-1 {
			break
		}

		for n := 1; n < edgeCount; n++ {
			if err := d.AddEdge(id, ids[index+n]); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for index, id := range ids {
		if index+edgeCount+1 > len(ids)-1 {
			break
		}

		if err := d.AddEdge(id, ids[index+edgeCount+1]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDAGDeleteEdge(b *testing.B) {
	var ids []string
	d := NewDAG[string]()
	for n := 0; n < b.N; n++ {
		id := fmt.Sprint(n)
		if err := d.AddVertex(id, string(id)); err != nil {
			b.Fatal(err)
		}

		ids = append(ids, id)
	}

	for index, id := range ids {
		if index < 1 {
			continue
		}

		if err := d.AddEdge(id, ids[index-1]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for index, id := range ids {
		if index < 1 {
			continue
		}

		if err := d.DeleteEdge(id, ids[index-1]); err != nil {
			b.Fatal(err)
		}
	}
}
