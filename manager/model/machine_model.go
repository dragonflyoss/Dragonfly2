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

package model

import "encoding/json"

type MachineModel struct {
	ID      uint               `json:"id" binding:"required"`      //模型对应的scheduler的id
	Params  map[string]float64 `json:"params" binding:"required"`  //线形模型的参数
	Version string             `json:"version" binding:"required"` //模型版本号
}

type Version struct {
	VersionID string  `json:"version_id" binding:"required"`
	Precision float64 `json:"precision" binding:"required"`
	Recall    float64 `json:"recall" binding:"required"`
}

func (m MachineModel) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m Version) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}
