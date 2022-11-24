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

package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"

	schedulerv1 "d7y.io/api/pkg/apis/scheduler/v1"
)

func TestResource_IsPieceBackToSource(t *testing.T) {
	tests := []struct {
		name   string
		piece  *schedulerv1.PieceResult
		expect func(t *testing.T, ok bool)
	}{
		{
			name: "piece is back-to-source",
			piece: &schedulerv1.PieceResult{
				DstPid: "",
			},
			expect: func(t *testing.T, ok bool) {
				assert := assert.New(t)
				assert.True(ok)
			},
		},
		{
			name: "piece is not back-to-source",
			piece: &schedulerv1.PieceResult{
				DstPid: "foo",
			},
			expect: func(t *testing.T, ok bool) {
				assert := assert.New(t)
				assert.False(ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.expect(t, IsPieceBackToSource(tc.piece))
		})
	}
}
