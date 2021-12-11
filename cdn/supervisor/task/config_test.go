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

package task

import (
	"reflect"
	"testing"
	"time"
)

func TestConfig_applyDefaults(t *testing.T) {
	type fields struct {
		GCInitialDelay     time.Duration
		GCMetaInterval     time.Duration
		TaskExpireTime     time.Duration
		FailAccessInterval time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   Config
	}{
		{
			name:   "empty config ",
			fields: fields{},
			want: Config{
				GCInitialDelay:     DefaultGCInitialDelay,
				GCMetaInterval:     DefaultGCMetaInterval,
				TaskExpireTime:     DefaultTaskExpireTime,
				FailAccessInterval: DefaultFailAccessInterval,
			},
		}, {
			name: "unset GCInitialDelay",
			fields: fields{
				GCMetaInterval:     2 * time.Second,
				TaskExpireTime:     3 * time.Minute,
				FailAccessInterval: 5 * time.Minute,
			},
			want: Config{
				GCInitialDelay:     DefaultGCInitialDelay,
				GCMetaInterval:     2 * time.Second,
				TaskExpireTime:     3 * time.Minute,
				FailAccessInterval: 5 * time.Minute,
			},
		}, {
			name: "unset GCMetaInterval",
			fields: fields{
				GCInitialDelay:     2 * time.Second,
				TaskExpireTime:     3 * time.Minute,
				FailAccessInterval: 5 * time.Minute,
			},
			want: Config{
				GCInitialDelay:     2 * time.Second,
				GCMetaInterval:     DefaultGCMetaInterval,
				TaskExpireTime:     3 * time.Minute,
				FailAccessInterval: 5 * time.Minute,
			},
		}, {
			name: "unset TaskExpireTime",
			fields: fields{
				GCInitialDelay:     2 * time.Second,
				GCMetaInterval:     2 * time.Second,
				FailAccessInterval: 5 * time.Minute,
			},
			want: Config{
				GCInitialDelay:     2 * time.Second,
				GCMetaInterval:     2 * time.Second,
				TaskExpireTime:     DefaultTaskExpireTime,
				FailAccessInterval: 5 * time.Minute,
			},
		}, {
			name: "unset FailAccessInterval",
			fields: fields{
				GCInitialDelay: 2 * time.Second,
				GCMetaInterval: 2 * time.Second,
				TaskExpireTime: 3 * time.Minute,
			},
			want: Config{
				GCInitialDelay:     2 * time.Second,
				GCMetaInterval:     2 * time.Second,
				TaskExpireTime:     3 * time.Minute,
				FailAccessInterval: DefaultFailAccessInterval,
			},
		}, {
			name: "custom task config",
			fields: fields{
				GCInitialDelay:     2 * time.Second,
				GCMetaInterval:     2 * time.Second,
				TaskExpireTime:     3 * time.Minute,
				FailAccessInterval: 5 * time.Minute,
			},
			want: Config{
				GCInitialDelay:     2 * time.Second,
				GCMetaInterval:     2 * time.Second,
				TaskExpireTime:     3 * time.Minute,
				FailAccessInterval: 5 * time.Minute,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Config{
				GCInitialDelay:     tt.fields.GCInitialDelay,
				GCMetaInterval:     tt.fields.GCMetaInterval,
				TaskExpireTime:     tt.fields.TaskExpireTime,
				FailAccessInterval: tt.fields.FailAccessInterval,
			}
			if got := c.applyDefaults(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applyDefaults() = %v, want %v", got, tt.want)
			}
		})
	}
}
