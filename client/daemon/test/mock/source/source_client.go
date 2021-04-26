// Code generated by MockGen. DO NOT EDIT.
// Source: ../../../../../cdnsystem/source/source_client.go

// Package source is a generated GoMock package.
package source

import (
	io "io"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockResourceClient is a mock of ResourceClient interface.
type MockResourceClient struct {
	ctrl     *gomock.Controller
	recorder *MockResourceClientMockRecorder
}

// MockResourceClientMockRecorder is the mock recorder for MockResourceClient.
type MockResourceClientMockRecorder struct {
	mock *MockResourceClient
}

// NewMockResourceClient creates a new mock instance.
func NewMockResourceClient(ctrl *gomock.Controller) *MockResourceClient {
	mock := &MockResourceClient{ctrl: ctrl}
	mock.recorder = &MockResourceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockResourceClient) EXPECT() *MockResourceClientMockRecorder {
	return m.recorder
}

// Download mocks base method.
func (m *MockResourceClient) Download(url string, headers map[string]string) (io.ReadCloser, map[string]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Download", url, headers)
	ret0, _ := ret[0].(io.ReadCloser)
	ret1, _ := ret[1].(map[string]string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Download indicates an expected call of Download.
func (mr *MockResourceClientMockRecorder) Download(url, headers interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Download", reflect.TypeOf((*MockResourceClient)(nil).Download), url, headers)
}

// GetContentLength mocks base method.
func (m *MockResourceClient) GetContentLength(url string, headers map[string]string) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetContentLength", url, headers)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetContentLength indicates an expected call of GetContentLength.
func (mr *MockResourceClientMockRecorder) GetContentLength(url, headers interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetContentLength", reflect.TypeOf((*MockResourceClient)(nil).GetContentLength), url, headers)
}

// IsExpired mocks base method.
func (m *MockResourceClient) IsExpired(url string, headers, expireInfo map[string]string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsExpired", url, headers, expireInfo)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsExpired indicates an expected call of IsExpired.
func (mr *MockResourceClientMockRecorder) IsExpired(url, headers, expireInfo interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsExpired", reflect.TypeOf((*MockResourceClient)(nil).IsExpired), url, headers, expireInfo)
}

// IsSupportRange mocks base method.
func (m *MockResourceClient) IsSupportRange(url string, headers map[string]string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsSupportRange", url, headers)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// IsSupportRange indicates an expected call of IsSupportRange.
func (mr *MockResourceClientMockRecorder) IsSupportRange(url, headers interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsSupportRange", reflect.TypeOf((*MockResourceClient)(nil).IsSupportRange), url, headers)
}
