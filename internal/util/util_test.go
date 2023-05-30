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
			name: "length equal 200M and get default piece size",
			args: args{
				length: 200 * 1024 * 1024,
			},
			want: DefaultPieceSize,
		}, {
			name: "length smaller than 200M and get default piece size",
			args: args{
				length: 100 * 1024 * 1024,
			},
			want: DefaultPieceSize,
		}, {
			name: "length greater than 200M",
			args: args{
				length: 205 * 1024 * 1024,
			},
			want: DefaultPieceSize,
		}, {
			name: "length greater than 300M",
			args: args{
				length: 310 * 1024 * 1024,
			},
			want: DefaultPieceSize + 1*1024*1024,
		}, {
			name: "length reach piece size limit",
			args: args{
				length: 3100 * 1024 * 1024,
			},
			want: DefaultPieceSizeLimit,
		}, {
			name: "500M+ length",
			args: args{
				length: 552562021,
			},
			want: DefaultPieceSize + 3*1024*1024,
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

func TestComputePieceCount(t *testing.T) {
	type args struct {
		length int64
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		{
			name: "less than one piece",
			args: args{
				length: DefaultPieceSize - 1,
			},
			want: 1,
		},
		{
			name: "extra incomplete piece",
			args: args{
				length: DefaultPieceSize + (DefaultPieceSize - 1),
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ComputePieceCount(tt.args.length, DefaultPieceSize); got != tt.want {
				t.Errorf("ComputePieceCount() = %v, want %v", got, tt.want)
			}
		})
	}
}
