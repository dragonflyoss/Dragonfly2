/*
 *     Copyright 2020 The Dragonfly Authors
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

package plugins

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestPluginsTestSuite(t *testing.T) {
	suite.Run(t, new(PluginsTestSuite))
}

type PluginsTestSuite struct {
	suite.Suite
	mgr Manager
}

func (s *PluginsTestSuite) SetUpSuite() {
	s.mgr = mgr
}

func (s *PluginsTestSuite) TearDownSuite() {
	mgr = s.mgr
}

func (s *PluginsTestSuite) TearDownTest() {
	mgr = s.mgr
}

func (s *PluginsTestSuite) TestPluginBuilder() {
	var builder Builder = func(conf interface{}) (plugin Plugin, e error) {
		return nil, nil
	}
	manager := NewManager()

	var testFunc = func(pt PluginType, name string, b Builder, result bool) {
		err := manager.AddBuilder(pt, name, b)
		s.Require().Nil(err)
		obj, _ := manager.GetBuilder(pt, name)
		if result {
			s.NotNil(obj)
			objVal := reflect.ValueOf(obj)
			bVal := reflect.ValueOf(b)
			s.Equal(objVal.Pointer(), bVal.Pointer())
			manager.DeleteBuilder(pt, name)
		} else {
			s.Nil(obj)
		}
	}

	for _, pt := range PluginTypes {
		testFunc(pt, "test", builder, true)
		testFunc(pt, "", nil, false)
		testFunc(pt, "", builder, false)
		testFunc(pt, "test", nil, false)
	}
}

func (s *PluginsTestSuite) TestManagerPlugin() {
	manager := NewManager()

	var testFunc = func(p Plugin, result bool) {
		err := manager.AddPlugin(p)
		if !result {
			s.NotNil(err)
		}
		obj, ok := manager.GetPlugin(p.Type(), p.Name())
		if ok {
			s.NotNil(obj)
			s.Equal(obj, p)
			manager.DeletePlugin(p.Type(), p.Name())
		} else {
			s.Nil(obj)
		}
	}

	testFunc(&mockPlugin{"test", "test"}, false)
	for _, pt := range PluginTypes {
		testFunc(&mockPlugin{pt, "test"}, true)
		testFunc(&mockPlugin{pt, ""}, false)
	}
}

func (s *PluginsTestSuite) TestRepositoryIml() {
	type testCase struct {
		pt        PluginType
		name      string
		data      interface{}
		addResult bool
	}
	var createCase = func(validPlugin bool, name string, data interface{}, result bool) testCase {
		pt := StorageDriverPlugin
		if !validPlugin {
			pt = PluginType("test-validPlugin")
		}
		return testCase{
			pt:        pt,
			name:      name,
			data:      data,
			addResult: result,
		}
	}
	var tc = func(valid bool, name string, data interface{}) testCase {
		return createCase(valid, name, data, true)
	}
	var fc = func(valid bool, name string, data interface{}) testCase {
		return createCase(valid, name, data, false)
	}
	var cases = []testCase{
		fc(true, "test", nil),
		fc(true, "", "data"),
		fc(false, "test", "data"),
		tc(true, "test", "data"),
	}

	repo := NewRepository()
	for _, v := range cases {
		err := repo.Add(v.pt, v.name, v.data)
		s.Require().Nil(err)
		data, _ := repo.Get(v.pt, v.name)
		if v.addResult {
			s.NotNil(data)
			s.Equal(data, v.data)
			repo.Delete(v.pt, v.name)
			data, _ = repo.Get(v.pt, v.name)
			s.Nil(data)
		} else {
			s.Nil(data)
		}
	}
}

func (s *PluginsTestSuite) TestValidate() {
	type testCase struct {
		pt       PluginType
		name     string
		expected bool
	}
	var cases = []testCase{
		{PluginType("test"), "", false},
		{PluginType("test"), "test", false},
	}
	for _, pt := range PluginTypes {
		cases = append(cases,
			testCase{pt, "", false},
			testCase{pt, "test", true},
		)
	}
	for _, v := range cases {
		s.Equal(validate(v.pt, v.name), v.expected)
	}
}

// -----------------------------------------------------------------------------

type mockPlugin struct {
	pt   PluginType
	name string
}

func (m *mockPlugin) Type() PluginType {
	return m.pt
}

func (m *mockPlugin) Name() string {
	return m.name
}
