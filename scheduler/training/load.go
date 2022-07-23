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

package training

import (
	"bytes"
	"encoding/gob"
)

type Loading struct {
}

func (strategy *Loading) Serve(data []byte) (map[float64]*LinearModel, error) {
	var result map[float64]*LinearModel
	dec := gob.NewDecoder(bytes.NewBuffer(data)) // 创建一个对象 把需要转化的对象放入
	err := dec.Decode(&result)                   // 进行流转化
	if err != nil {
		return nil, err
	}
	return result, nil
}
