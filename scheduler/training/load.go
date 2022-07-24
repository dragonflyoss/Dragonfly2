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

	"d7y.io/dragonfly/v2/scheduler/pipeline"
)

type LoadSource func(map[float64]*LinearModel) ([]byte, error)

type Loading struct {
	data   []byte
	source LoadSource
	name   string
}

// 实际方法
func (load *Loading) GetSource() (map[float64]*LinearModel, error) {
	//TODO save方法改造
	source, err := load.source(map[float64]*LinearModel{})
	if err != nil {
		return nil, err
	}
	load.data = source

	var result map[float64]*LinearModel
	dec := gob.NewDecoder(bytes.NewBuffer(load.data)) // 创建一个对象 把需要转化的对象放入
	err = dec.Decode(&result)                         // 进行流转化
	if err != nil {
		return nil, err
	}
	return result, nil
}

// 封装的serve接口
func (load *Loading) Serve() {
	_, err := load.GetSource()
	if err != nil {
		return
	}
}

func NewLoad() *Loading {
	return &Loading{
		source: RegisterSaving,
		name:   "loading",
	}
}

func NewLoadStep() pipeline.Step {
	return &trainingStep{
		Stest: NewLoad(),
	}
}

func init() {
	// TODO index待确定
	pipeline.BuildMap[0] = NewLoadStep
}
