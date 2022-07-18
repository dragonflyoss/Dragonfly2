// Code generated by MockGen. DO NOT EDIT.
// Source: dag.go

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	dag "d7y.io/dragonfly/v2/pkg/dag"
	gomock "github.com/golang/mock/gomock"
)

// MockDAG is a mock of DAG interface.
type MockDAG struct {
	ctrl     *gomock.Controller
	recorder *MockDAGMockRecorder
}

// MockDAGMockRecorder is the mock recorder for MockDAG.
type MockDAGMockRecorder struct {
	mock *MockDAG
}

// NewMockDAG creates a new mock instance.
func NewMockDAG(ctrl *gomock.Controller) *MockDAG {
	mock := &MockDAG{ctrl: ctrl}
	mock.recorder = &MockDAGMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDAG) EXPECT() *MockDAGMockRecorder {
	return m.recorder
}

// AddEdge mocks base method.
func (m *MockDAG) AddEdge(fromVertexID, toVertexID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddEdge", fromVertexID, toVertexID)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddEdge indicates an expected call of AddEdge.
func (mr *MockDAGMockRecorder) AddEdge(fromVertexID, toVertexID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddEdge", reflect.TypeOf((*MockDAG)(nil).AddEdge), fromVertexID, toVertexID)
}

// AddVertex mocks base method.
func (m *MockDAG) AddVertex(id string, value any) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddVertex", id, value)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddVertex indicates an expected call of AddVertex.
func (mr *MockDAGMockRecorder) AddVertex(id, value interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddVertex", reflect.TypeOf((*MockDAG)(nil).AddVertex), id, value)
}

// DeleteEdge mocks base method.
func (m *MockDAG) DeleteEdge(fromVertexID, toVertexID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteEdge", fromVertexID, toVertexID)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteEdge indicates an expected call of DeleteEdge.
func (mr *MockDAGMockRecorder) DeleteEdge(fromVertexID, toVertexID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteEdge", reflect.TypeOf((*MockDAG)(nil).DeleteEdge), fromVertexID, toVertexID)
}

// DeleteVertex mocks base method.
func (m *MockDAG) DeleteVertex(id string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "DeleteVertex", id)
}

// DeleteVertex indicates an expected call of DeleteVertex.
func (mr *MockDAGMockRecorder) DeleteVertex(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteVertex", reflect.TypeOf((*MockDAG)(nil).DeleteVertex), id)
}

// GetVertex mocks base method.
func (m *MockDAG) GetVertex(id string) (*dag.Vertex, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVertex", id)
	ret0, _ := ret[0].(*dag.Vertex)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVertex indicates an expected call of GetVertex.
func (mr *MockDAGMockRecorder) GetVertex(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVertex", reflect.TypeOf((*MockDAG)(nil).GetVertex), id)
}
