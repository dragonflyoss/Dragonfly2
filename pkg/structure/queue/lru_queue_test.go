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

package queue

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestLruQueue(t *testing.T) {
	suite.Run(t, new(LruQueueTestSuite))
}

type LruQueueTestSuite struct {
	suite.Suite
}

func (suite *LruQueueTestSuite) TestLRUQueue() {
	q := NewLRUQueue(5)

	q.Put("key1", 1)

	v1, err := q.GetItemByKey("key1")
	suite.Nil(err)
	suite.Equal(v1.(int), 1)

	items := q.GetFront(1)
	suite.Equal(len(items), 1)
	suite.Equal(items[0], 1)

	q.Put("key2", 2)
	q.Put("key1", 3)

	v1, err = q.GetItemByKey("key1")
	suite.Nil(err)
	suite.Equal(v1.(int), 3)

	items = q.GetFront(10)
	suite.Equal(len(items), 2)
	suite.Equal(items[0], 3)
	suite.Equal(items[1], 2)

	items = q.GetFront(1)
	suite.Equal(len(items), 1)
	suite.Equal(items[0], 3)

	_, err = q.GetItemByKey("key3")
	suite.NotNil(err)

	obsoleteKey, _ := q.Put("key3", "data3")
	suite.Equal(obsoleteKey, "")
	obsoleteKey, _ = q.Put("key4", "data4")
	suite.Equal(obsoleteKey, "")
	obsoleteKey, _ = q.Put("key5", "data5")
	suite.Equal(obsoleteKey, "")

	items = q.GetFront(10)
	suite.Equal(len(items), 5)
	suite.Equal(items[0], "data5")
	suite.Equal(items[1], "data4")
	suite.Equal(items[2], "data3")
	suite.Equal(items[3], 3)
	suite.Equal(items[4], 2)

	obsoleteKey, obsoleteData := q.Put("key6", "data6")
	suite.Equal(obsoleteKey, "key2")
	suite.Equal(obsoleteData.(int), 2)
	_, err = q.GetItemByKey("key2")
	suite.NotNil(err)

	items = q.GetFront(5)
	suite.Equal(len(items), 5)
	suite.Equal(items[0], "data6")
	suite.Equal(items[1], "data5")
	suite.Equal(items[2], "data4")
	suite.Equal(items[3], "data3")
	suite.Equal(items[4], 3)

	v1, err = q.Get("key5")
	suite.Nil(err)
	suite.Equal(v1.(string), "data5")

	items = q.GetFront(5)
	suite.Equal(len(items), 5)
	suite.Equal(items[0], "data5")
	suite.Equal(items[1], "data6")
	suite.Equal(items[2], "data4")
	suite.Equal(items[3], "data3")
	suite.Equal(items[4], 3)

	v1 = q.Delete("key3")
	suite.Equal(v1, "data3")

	items = q.GetFront(5)
	suite.Equal(len(items), 4)
	suite.Equal(items[0], "data5")
	suite.Equal(items[1], "data6")
	suite.Equal(items[2], "data4")
	suite.Equal(items[3], 3)

	v1 = q.Delete("key3")
	suite.Nil(v1)
}
