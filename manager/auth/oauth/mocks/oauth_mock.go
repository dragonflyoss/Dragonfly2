// Code generated by MockGen. DO NOT EDIT.
// Source: oauth.go
//
// Generated by this command:
//
//	mockgen -destination mocks/oauth_mock.go -source oauth.go -package mocks
//

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	oauth "d7y.io/dragonfly/v2/manager/auth/oauth"
	gomock "go.uber.org/mock/gomock"
	oauth2 "golang.org/x/oauth2"
)

// MockOauth is a mock of Oauth interface.
type MockOauth struct {
	ctrl     *gomock.Controller
	recorder *MockOauthMockRecorder
}

// MockOauthMockRecorder is the mock recorder for MockOauth.
type MockOauthMockRecorder struct {
	mock *MockOauth
}

// NewMockOauth creates a new mock instance.
func NewMockOauth(ctrl *gomock.Controller) *MockOauth {
	mock := &MockOauth{ctrl: ctrl}
	mock.recorder = &MockOauthMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockOauth) EXPECT() *MockOauthMockRecorder {
	return m.recorder
}

// AuthCodeURL mocks base method.
func (m *MockOauth) AuthCodeURL() (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AuthCodeURL")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AuthCodeURL indicates an expected call of AuthCodeURL.
func (mr *MockOauthMockRecorder) AuthCodeURL() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AuthCodeURL", reflect.TypeOf((*MockOauth)(nil).AuthCodeURL))
}

// Exchange mocks base method.
func (m *MockOauth) Exchange(arg0 string) (*oauth2.Token, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Exchange", arg0)
	ret0, _ := ret[0].(*oauth2.Token)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Exchange indicates an expected call of Exchange.
func (mr *MockOauthMockRecorder) Exchange(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exchange", reflect.TypeOf((*MockOauth)(nil).Exchange), arg0)
}

// GetUser mocks base method.
func (m *MockOauth) GetUser(arg0 *oauth2.Token) (*oauth.User, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUser", arg0)
	ret0, _ := ret[0].(*oauth.User)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetUser indicates an expected call of GetUser.
func (mr *MockOauthMockRecorder) GetUser(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUser", reflect.TypeOf((*MockOauth)(nil).GetUser), arg0)
}
