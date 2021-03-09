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

package hashcircler

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestSuite(t *testing.T) {
	suite.Run(t, &hashCirclerSuite{
		hashMap: map[string]uint64{},
	})
}

type hashCirclerSuite struct {
	hashMap map[string]uint64
	suite.Suite
}

func (suite *hashCirclerSuite) registerHashKV(key string, value uint64) {
	suite.hashMap[key] = value
}

func (suite *hashCirclerSuite) unRegisterHashKV(key string) {
	delete(suite.hashMap, key)
}

func (suite *hashCirclerSuite) cleanHashMap() {
	suite.hashMap = make(map[string]uint64)
}

func (suite *hashCirclerSuite) hash(input string) uint64 {
	v, ok := suite.hashMap[input]
	if ok {
		return v
	}

	return 0
}

func (suite *hashCirclerSuite) TestHashCircler() {
	defer suite.cleanHashMap()

	rangeSize := uint64(math.MaxUint64 / 5)
	suite.registerHashKV("v1", rand.Uint64()%rangeSize)
	suite.registerHashKV("v2", rand.Uint64()%rangeSize)
	suite.registerHashKV("v3", rand.Uint64()%rangeSize+rangeSize)
	suite.registerHashKV("v4", rand.Uint64()%rangeSize+rangeSize)
	suite.registerHashKV("v5", rand.Uint64()%rangeSize+rangeSize*2)
	suite.registerHashKV("v6", rand.Uint64()%rangeSize+rangeSize*2)
	suite.registerHashKV("v7", rand.Uint64()%rangeSize+rangeSize*3)
	suite.registerHashKV("v8", rand.Uint64()%rangeSize+rangeSize*3)
	suite.registerHashKV("v9", rand.Uint64()%rangeSize+rangeSize*4)
	suite.registerHashKV("v10", rand.Uint64()%rangeSize+rangeSize*4)

	arr := []string{
		"key1", "key2", "key3", "key4", "key5",
	}

	inputStrs := []string{
		"v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
	}

	hasher, err := NewConsistentHashCircler(arr, nil)
	suite.Nil(err)

	originKeys := make([]string, len(inputStrs))

	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)
		originKeys[i] = k
	}

	// disable arr[0]
	hasher.Delete(arr[0])
	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)
		suite.NotEqual(k, arr[0])
		if originKeys[i] != arr[0] {
			suite.Equal(k, originKeys[i])
		}
	}

	hasher.Delete(arr[1])
	hasher.Delete(arr[2])
	hasher.Delete(arr[4])

	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)
		suite.Equal(k, arr[3])
	}

	hasher.Add(arr[1])

	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)
		if originKeys[i] == arr[1] || originKeys[i] == arr[3] {
			suite.Equal(k, originKeys[i])
		}
		suite.Equal(true, k == arr[3] || k == arr[1])
	}

	hasher.Add(arr[1])
	hasher.Add(arr[2])

	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)

		if originKeys[i] == arr[1] || originKeys[i] == arr[2] || originKeys[i] == arr[3] {
			suite.Equal(k, originKeys[i])
		}

		suite.Equal(true, k != arr[0] && k != arr[4])
	}

	hasher.Delete(arr[0])
	hasher.Delete(arr[1])
	hasher.Delete(arr[2])
	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)
		suite.Equal(k, arr[3])
	}

	hasher.Delete(arr[3])
	for i := 0; i < 10; i++ {
		_, err = hasher.Hash(inputStrs[i])
		suite.NotNil(err)
	}

	hasher.Add(arr[0])
	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)
		suite.Equal(k, arr[0])
	}

	hasher.Add(arr[1])
	hasher.Add(arr[2])
	hasher.Add(arr[3])
	hasher.Add(arr[4])

	for i := 0; i < 10; i++ {
		k, err := hasher.Hash(inputStrs[i])
		suite.Nil(err)
		suite.Equal(k, originKeys[i])
	}
}
