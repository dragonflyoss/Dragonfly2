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

package source

import (
	"fmt"
	"testing"
)

func TestHeader_Clone(t *testing.T) {
	tests := []struct {
		name string
		h    Header
		want Header
	}{
		{
			name: "",
			h: Header{
				"aaa": []string{"ddd"}, "fff": []string{"cccc"},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xx := tt.h.Clone()
			xx["aaa"] = []string{"bbbb"}
			fmt.Println(xx["aaa"])
			fmt.Println(tt.h["aaa"])
		})
	}
}
