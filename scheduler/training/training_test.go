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
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/sjwhitworth/golearn/base"
	"github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/scheduler/storage"
)

func TestTraining_New(t *testing.T) {
	tests := []struct {
		name    string
		baseDir string
		expect  func(t *testing.T, s Training, err error)
	}{
		{
			name:    "new training module",
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
			s, err := New(tc.baseDir, "record.csv")
			tc.expect(t, s, err)
		})
	}
}

func TestTraining_Process(t *testing.T) {
	sto, _ := storage.New(os.TempDir())
	rand.Seed(time.Now().Unix())

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T)
		expect1 func(t *testing.T, r map[float64]*base.DenseInstances, err error)
		expect2 func(t *testing.T, r map[float64]*LinearModel, err error)
		expect3 func(t *testing.T, r map[float64]*LinearModel)
	}{
		{
			name:    "random record preprocess",
			baseDir: os.TempDir(),
			mock: func(t *testing.T) {
				for i := 0; i < 10100; i++ {
					record := storage.Record{
						ID:             "1",
						IP:             rand.Intn(100)%2 + 1,
						HostName:       rand.Intn(100)%2 + 1,
						Tag:            rand.Intn(100)%2 + 1,
						Rate:           float64(rand.Intn(300) + 10),
						ParentPiece:    float64(rand.Intn(240) + 14),
						SecurityDomain: rand.Intn(100)%2 + 1,
						IDC:            rand.Intn(100)%2 + 1,
						NetTopology:    rand.Intn(100)%2 + 1,
						Location:       rand.Intn(100)%2 + 1,
						UploadRate:     float64(rand.Intn(550) + 3),
						State:          rand.Intn(4),
						CreateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
						UpdateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
						ParentID:       strconv.Itoa(rand.Intn(5)),
						ParentCreateAt: time.Now().Unix()/7200 + rand.Int63n(10),
						ParentUpdateAt: time.Now().Unix()/7200 + rand.Int63n(10),
					}
					err := sto.Create(record)
					if err != nil {
						t.Fatal(err)
					}
				}
			},
			expect1: func(t *testing.T, r map[float64]*base.DenseInstances, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				lineSum := 0
				for key, value := range r {
					assert.True(key < 5)
					_, length := value.Size()
					lineSum += length
				}
				assert.Equal(lineSum, 10000)
			},
			expect2: func(t *testing.T, r map[float64]*LinearModel, err error) {
				assert := assert.New(t)
				assert.NoError(err)
				for _, value := range r {
					assert.True(!math.IsNaN(value.MAE))
					assert.True(!math.IsNaN(value.RMSE))
					assert.True(!math.IsNaN(value.MSE))
					assert.True(!math.IsNaN(value.R2))
				}
			},
			expect3: func(t *testing.T, r map[float64]*LinearModel) {
				assert := assert.New(t)
				saving := new(Saving)
				bytes, err := saving.Serve(r)
				if err != nil {
					assert.NoError(err)
				}
				loading := new(Loading)
				rMap, err := loading.Serve(bytes)
				if err != nil {
					assert.NoError(err)
				}
				for key, value1 := range rMap {
					value2, ok := r[key]
					assert.True(ok)
					assert.Equal(value1.RMSE, value2.RMSE)
					assert.Equal(value1.MAE, value2.MAE)
					assert.Equal(value1.MSE, value2.MSE)
					assert.Equal(value1.R2, value2.R2)
					assert.Equal(value1.Model.Disturbance, value2.Model.Disturbance)
					for i := 0; i < len(value1.Model.RegressionCoefficients); i++ {
						assert.Equal(value1.Model.RegressionCoefficients[i], value2.Model.RegressionCoefficients[i])
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := New(os.TempDir(), "record.csv")
			tc.mock(t)
			instance, err := s.PreProcess()
			tc.expect1(t, instance, err)
			result, err := s.TrainProcess(instance)
			tc.expect2(t, result, err)
			tc.expect3(t, result)
		})
	}
}
