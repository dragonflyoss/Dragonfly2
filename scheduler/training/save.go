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
	"fmt"
)

type Saving struct {
}

func (strategy *Saving) Serve(modelMap map[float64]*LinearModel) ([]byte, error) {
	buf := new(bytes.Buffer)    // 创建一个buffer区
	enc := gob.NewEncoder(buf)  // 创建新的需要转化二进制区域对象
	err := enc.Encode(modelMap) // 将数据转化为二进制流
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	b := buf.Bytes() // 将二进制流赋值给变量b
	return b, nil
}
