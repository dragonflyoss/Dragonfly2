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
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	mockVertexID    = "foo"
	mockVertexValue = "var"
)

func TestNewVertex(t *testing.T) {
	v := NewVertex(mockVertexID, mockVertexValue)
	assert := assert.New(t)
	assert.Equal(v.ID, mockVertexID)
	assert.Equal(v.Value, mockVertexValue)
	assert.Equal(v.Parents.Len(), uint(0))
	assert.Equal(v.Children.Len(), uint(0))
}

func TestVertexDegree(t *testing.T) {
	v := NewVertex(mockVertexID, mockVertexValue)
	assert := assert.New(t)
	assert.Equal(v.ID, mockVertexID)
	assert.Equal(v.Value, mockVertexValue)
	assert.Equal(v.Degree(), 0)

	v.Parents.Add(mockVertexID)
	assert.Equal(v.Degree(), 1)

	v.Children.Add(mockVertexID)
	assert.Equal(v.Degree(), 2)

	v.Parents.Delete(mockVertexID)
	assert.Equal(v.Degree(), 1)

	v.Children.Delete(mockVertexID)
	assert.Equal(v.Degree(), 0)
}

func TestVertexInDegree(t *testing.T) {
	v := NewVertex(mockVertexID, mockVertexValue)
	assert := assert.New(t)
	assert.Equal(v.ID, mockVertexID)
	assert.Equal(v.Value, mockVertexValue)
	assert.Equal(v.InDegree(), 0)

	v.Parents.Add(mockVertexID)
	assert.Equal(v.InDegree(), 1)

	v.Children.Add(mockVertexID)
	assert.Equal(v.InDegree(), 1)

	v.Parents.Delete(mockVertexID)
	assert.Equal(v.InDegree(), 0)

	v.Children.Delete(mockVertexID)
	assert.Equal(v.InDegree(), 0)
}

func TestVertexOutDegree(t *testing.T) {
	v := NewVertex(mockVertexID, mockVertexValue)
	assert := assert.New(t)
	assert.Equal(v.ID, mockVertexID)
	assert.Equal(v.Value, mockVertexValue)
	assert.Equal(v.OutDegree(), 0)

	v.Parents.Add(mockVertexID)
	assert.Equal(v.OutDegree(), 0)

	v.Children.Add(mockVertexID)
	assert.Equal(v.OutDegree(), 1)

	v.Parents.Delete(mockVertexID)
	assert.Equal(v.OutDegree(), 1)

	v.Children.Delete(mockVertexID)
	assert.Equal(v.OutDegree(), 0)
}

func TestVertexDeleteInEdges(t *testing.T) {
	va := NewVertex(mockVertexID, mockVertexValue)
	vb := NewVertex(mockVertexID, mockVertexValue)
	assert := assert.New(t)

	va.Parents.Add(vb)
	vb.Children.Add(va)
	assert.Equal(va.Parents.Len(), uint(1))
	assert.Equal(vb.Children.Len(), uint(1))

	va.DeleteInEdges()
	assert.Equal(va.Parents.Len(), uint(0))
	assert.Equal(vb.Children.Len(), uint(0))
}

func TestVertexDeleteOutEdges(t *testing.T) {
	va := NewVertex(mockVertexID, mockVertexValue)
	vb := NewVertex(mockVertexID, mockVertexValue)
	assert := assert.New(t)

	va.Parents.Add(vb)
	vb.Children.Add(va)
	assert.Equal(va.Parents.Len(), uint(1))
	assert.Equal(vb.Children.Len(), uint(1))

	vb.DeleteOutEdges()
	assert.Equal(va.Parents.Len(), uint(0))
	assert.Equal(vb.Children.Len(), uint(0))
}
