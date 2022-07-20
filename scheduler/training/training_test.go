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
	"d7y.io/dragonfly/v2/scheduler/storage"
	"fmt"
	"github.com/sjwhitworth/golearn/base"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestTraining_New(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		expect  func(t *testing.T, s Training, err error)
	}{
		{
			name:    "new training module.",
			baseDir: os.TempDir(),
			expect: func(t *testing.T, s Training, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "training")
				assert.Equal(s.(*training).maxRecordLine, DefaultMaxRecordLine)
				assert.Equal(s.(*training).maxBufferLine, DefaultMaxBufferLine)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(tc.baseDir)
			tc.expect(t, s, err)
		})
	}
}

func TestStorage_Saving(t *testing.T) {
	sto, _ := storage.New(os.TempDir())
	record := storage.Record{
		ID:             "1",
		IP:             0,
		HostName:       1,
		Tag:            0,
		Rate:           13,
		ParentPiece:    23,
		SecurityDomain: 0,
		IDC:            1,
		NetTopology:    1,
		Location:       0,
		UploadRate:     13,
		State:          4,
		CreateAt:       time.Now().Unix() / 7200,
		UpdateAt:       time.Now().Unix() / 7200,
		ParentID:       "2",
		ParentCreateAt: time.Now().Unix() / 7200,
		ParentUpdateAt: time.Now().Unix() / 7200,
	}
	for i := 0; i < 130; i++ {
		err := sto.Create(record)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func TestTraining_PreProcess(t *testing.T) {
	s, _ := New(os.TempDir())
	tests := []struct {
		name    string
		baseDir string
		expect  func(t *testing.T, i *base.DenseInstances, err error)
	}{
		{
			name:    "new training module.",
			baseDir: os.TempDir(),
			expect: func(t *testing.T, i *base.DenseInstances, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				assert.Equal(reflect.TypeOf(s).Elem().Name(), "training")
				assert.Equal(s.(*training).maxRecordLine, DefaultMaxRecordLine)
				assert.Equal(s.(*training).maxBufferLine, DefaultMaxBufferLine)
				_, length := i.Size()
				assert.Equal(length, 64)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			instance, err := s.PreProcess()
			tc.expect(t, instance, err)
		})
	}
}
