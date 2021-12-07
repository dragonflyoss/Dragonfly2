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

package util

import (
	"testing"
)

func TestComputePieceSize(t *testing.T) {
	type args struct {
		length int64
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "length is equal to 256M and get default piece size",
			args: args{
				length: 256 * 1024 * 1024,
			},
			want: PieceSizeLowerBound,
		}, {
			name: "length is smaller than 256M and get default piece size",
			args: args{
				length: 128 * 1024 * 1024,
			},
			want: PieceSizeLowerBound,
		}, {
			name: "length is greater than 256M but smaller than 1G #1",
			args: args{
				length: (256 + 64) * 1024 * 1024,
			},
			want: PieceSizeLowerBound + 1024*1024,
		}, {
			name: "length is greater than 256M but smaller than 1G #2",
			args: args{
				length: 512 * 1024 * 1024,
			},
			want: PieceSizeLowerBound * 2,
		}, {
			name: "length is equal to 1G",
			args: args{
				length: 1024 * 1024 * 1024,
			},
			want: PieceSizeUpperBound,
		}, {
			name: "length reach piece size limit",
			args: args{
				length: 2056 * 1024 * 1024,
			},
			want: PieceSizeUpperBound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ComputePieceSize(tt.args.length); got != tt.want {
				t.Errorf("ComputePieceSize() = %v, want %v", got, tt.want)
			}
		})
	}
}
