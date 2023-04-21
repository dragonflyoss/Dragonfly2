// Code generated by MockGen. DO NOT EDIT.
// Source: storage.go

// Package mocks is a generated GoMock package.
package mocks

import (
	io "io"
	reflect "reflect"

	storage "d7y.io/dragonfly/v2/scheduler/storage"
	gomock "github.com/golang/mock/gomock"
)

// MockStorage is a mock of Storage interface.
type MockStorage struct {
	ctrl     *gomock.Controller
	recorder *MockStorageMockRecorder
}

// MockStorageMockRecorder is the mock recorder for MockStorage.
type MockStorageMockRecorder struct {
	mock *MockStorage
}

// NewMockStorage creates a new mock instance.
func NewMockStorage(ctrl *gomock.Controller) *MockStorage {
	mock := &MockStorage{ctrl: ctrl}
	mock.recorder = &MockStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStorage) EXPECT() *MockStorageMockRecorder {
	return m.recorder
}

// ClearDownload mocks base method.
func (m *MockStorage) ClearDownload() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ClearDownload")
	ret0, _ := ret[0].(error)
	return ret0
}

// ClearDownload indicates an expected call of ClearDownload.
func (mr *MockStorageMockRecorder) ClearDownload() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ClearDownload", reflect.TypeOf((*MockStorage)(nil).ClearDownload))
}

// ClearNetworkTopology mocks base method.
func (m *MockStorage) ClearNetworkTopology() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ClearNetworkTopology")
	ret0, _ := ret[0].(error)
	return ret0
}

// ClearNetworkTopology indicates an expected call of ClearNetworkTopology.
func (mr *MockStorageMockRecorder) ClearNetworkTopology() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ClearNetworkTopology", reflect.TypeOf((*MockStorage)(nil).ClearNetworkTopology))
}

// CreateDownload mocks base method.
func (m *MockStorage) CreateDownload(arg0 storage.Download) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateDownload", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateDownload indicates an expected call of CreateDownload.
func (mr *MockStorageMockRecorder) CreateDownload(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateDownload", reflect.TypeOf((*MockStorage)(nil).CreateDownload), arg0)
}

// CreateNetworkTopology mocks base method.
func (m *MockStorage) CreateNetworkTopology(arg0 storage.NetworkTopology) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateNetworkTopology", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateNetworkTopology indicates an expected call of CreateNetworkTopology.
func (mr *MockStorageMockRecorder) CreateNetworkTopology(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateNetworkTopology", reflect.TypeOf((*MockStorage)(nil).CreateNetworkTopology), arg0)
}

// DownloadCount mocks base method.
func (m *MockStorage) DownloadCount() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DownloadCount")
	ret0, _ := ret[0].(int64)
	return ret0
}

// DownloadCount indicates an expected call of DownloadCount.
func (mr *MockStorageMockRecorder) DownloadCount() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DownloadCount", reflect.TypeOf((*MockStorage)(nil).DownloadCount))
}

// ListDownload mocks base method.
func (m *MockStorage) ListDownload() ([]storage.Download, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListDownload")
	ret0, _ := ret[0].([]storage.Download)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListDownload indicates an expected call of ListDownload.
func (mr *MockStorageMockRecorder) ListDownload() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListDownload", reflect.TypeOf((*MockStorage)(nil).ListDownload))
}

// ListNetworkTopology mocks base method.
func (m *MockStorage) ListNetworkTopology() ([]storage.NetworkTopology, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListNetworkTopology")
	ret0, _ := ret[0].([]storage.NetworkTopology)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListNetworkTopology indicates an expected call of ListNetworkTopology.
func (mr *MockStorageMockRecorder) ListNetworkTopology() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListNetworkTopology", reflect.TypeOf((*MockStorage)(nil).ListNetworkTopology))
}

// NetworkTopologyCount mocks base method.
func (m *MockStorage) NetworkTopologyCount() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NetworkTopologyCount")
	ret0, _ := ret[0].(int64)
	return ret0
}

// NetworkTopologyCount indicates an expected call of NetworkTopologyCount.
func (mr *MockStorageMockRecorder) NetworkTopologyCount() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NetworkTopologyCount", reflect.TypeOf((*MockStorage)(nil).NetworkTopologyCount))
}

// OpenDownload mocks base method.
func (m *MockStorage) OpenDownload() (io.ReadCloser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OpenDownload")
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OpenDownload indicates an expected call of OpenDownload.
func (mr *MockStorageMockRecorder) OpenDownload() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenDownload", reflect.TypeOf((*MockStorage)(nil).OpenDownload))
}

// OpenNetworkTopology mocks base method.
func (m *MockStorage) OpenNetworkTopology() (io.ReadCloser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OpenNetworkTopology")
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OpenNetworkTopology indicates an expected call of OpenNetworkTopology.
func (mr *MockStorageMockRecorder) OpenNetworkTopology() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OpenNetworkTopology", reflect.TypeOf((*MockStorage)(nil).OpenNetworkTopology))
}
