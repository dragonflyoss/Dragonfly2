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

package cdn

import "testing"

func TestGetBreakRange(t *testing.T) {
	type args struct {
		breakPoint       int64
		sourceFileLength int64
		taskRange        string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "breakPoint with range",
			args: args{
				breakPoint:       5,
				sourceFileLength: 200,
				taskRange:        "3-103",
			},
			want:    "8-103",
			wantErr: false,
		}, {
			name: "range with breakPoint 1",
			args: args{
				breakPoint:       1,
				sourceFileLength: 100,
				taskRange:        "0-100",
			},
			want:    "1-99",
			wantErr: false,
		}, {
			name: "breakpoint larger than length of download required",
			args: args{
				breakPoint:       101,
				sourceFileLength: 200,
				taskRange:        "100-300",
			},
			want:    "",
			wantErr: true,
		}, {
			name: "breakpoint is equal with length of download required",
			args: args{
				breakPoint:       100,
				sourceFileLength: 200,
				taskRange:        "100-300",
			},
			want:    "",
			wantErr: true,
		}, {
			name: "breakpoint is smaller than length of download required",
			args: args{
				breakPoint:       99,
				sourceFileLength: 200,
				taskRange:        "100-300",
			},
			want:    "199-199",
			wantErr: false,
		}, {
			name: "test2",
			args: args{
				breakPoint:       102760448,
				sourceFileLength: 552562021,
				taskRange:        "",
			},
			want:    "102760448-552562020",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getBreakRange(tt.args.breakPoint, tt.args.taskRange, tt.args.sourceFileLength)
			if (err != nil) != tt.wantErr {
				t.Errorf("getBreakRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getBreakRange() got = %v, want %v", got, tt.want)
			}
		})
	}
}
