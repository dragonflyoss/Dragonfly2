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

type Loading struct {
}

// 实际方法
func (load *Loading) GetSource(req *pipeline.Request) *pipeline.Result {
	source := req.Data.([]byte)

	var result map[float64]*LinearModel
	dec := gob.NewDecoder(bytes.NewBuffer(source)) // 创建一个对象 把需要转化的对象放入
	err := dec.Decode(&result)
	if err != nil {
		return &pipeline.Result{
			Error: err,
		}
	}

	return &pipeline.Result{
		Error:  nil,
		Data:   result,
		KeyVal: req.KeyVal,
	}
}

// 封装的serve接口
func (load *Loading) Serve(req *pipeline.Request) *pipeline.Result {
	return load.GetSource(req)
}

func NewLoadStep() pipeline.Step {
	return &trainingStep{
		TrainingStrategy: &Loading{},
		Name:             "loading",
	}
}

// for its sequential step, no need to stop
func (load *Loading) Stop() error {
	return nil
}

func init() {
	pipeline.BuildMap["loading"] = NewLoadStep
}
