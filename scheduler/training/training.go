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
	"bufio"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sjwhitworth/golearn/base"

	"d7y.io/dragonfly/v2/scheduler/training/models"
)

const (
	// NormalizedFieldNum field which needs normalization lies at the end of record.
	NormalizedFieldNum = 7

	// DefaultMaxBufferLine capacity of lines which read from record file.
	DefaultMaxBufferLine = 64

	// DefaultMaxRecordLine capacity of lines which local memory obtains.
	DefaultMaxRecordLine = 100000

	// TestSetPercent percent of test set.
	TestSetPercent = 0.2

	// DefaultLearningRate default learning rate of model.
	DefaultLearningRate = 0.01
)

type LinearModel struct {
	// Model actual model.
	Model *models.LinearRegression

	// MAE mean absolute error
	MAE float64

	// MSE mean square error
	MSE float64

	// RMSE Root Mean Square Error
	RMSE float64

	// R² coefficient of determination
	R2 float64
}

// Training is the interface used for train models.
type Training interface {
	// PreProcess load and clean data before training.
	PreProcess() (map[float64]*base.DenseInstances, error)

	// TrainProcess fit and evaluate models for each parent peer.
	TrainProcess(map[float64]*base.DenseInstances) (map[float64]*LinearModel, error)
}

// training provides training functions.
type training struct {
	baseDir  string
	fileName string
	buffer   string

	// maxBufferLine capacity of lines which reads from record once.
	maxBufferLine int

	// currentRecordLine capacity of lines which training has.
	currentRecordLine int

	// maxRecordLine capacity of lines which local memory obtains.
	maxRecordLine int

	// learningRate learning rate of model.
	learningRate float64
}

// New return a Training instance.
func New(baseDir string, fileName string) (Training, error) {
	t := &training{
		baseDir:       baseDir,
		fileName:      filepath.Join(baseDir, fileName),
		maxBufferLine: DefaultMaxBufferLine,
		maxRecordLine: DefaultMaxRecordLine,
		learningRate:  DefaultLearningRate,
	}

	file, err := os.Open(t.fileName)
	if err != nil {
		return nil, err
	}
	file.Close()

	return t, nil
}

// PreProcess load and clean data before training.
func (strategy *training) PreProcess() (map[float64]*base.DenseInstances, error) {
	// TODO using pipeline, load -> missing -> normalize -> split
	f, _ := os.Open(strategy.fileName)
	result := make(map[float64]*base.DenseInstances, 0)
	reader := bufio.NewReader(f)
	for {
		instance, err := strategy.loadRecord(reader)
		if err != nil {
			if err.Error() == "file empty" {
				break
			}
			return nil, err
		}
		effectiveArr, err := strategy.missingValue(instance)
		if err != nil {
			return nil, err
		}
		err = strategy.normalize(instance, effectiveArr, false)
		if err != nil {
			return nil, err
		}
		err = strategy.split(instance, effectiveArr, result)
		if err != nil {
			return nil, err
		}
	}
	f.Close()
	return result, nil
}

// TrainProcess fit and evaluate models for each parent peer.
func (strategy *training) TrainProcess(trainMap map[float64]*base.DenseInstances) (map[float64]*LinearModel, error) {
	result := make(map[float64]*LinearModel)
	for key, value := range trainMap {
		model := models.NewLinearRegression()
		train, test := base.InstancesTrainTestSplit(value, TestSetPercent)
		err := model.Fit(train, strategy.learningRate)
		if err != nil {
			return nil, err
		}
		out, err := model.Predict(test)
		if err != nil {
			return nil, err
		}
		mae, mse, rmse, r2, err := strategy.evaluate(out, test)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(mae) || math.IsNaN(mse) || math.IsNaN(rmse) || math.IsNaN(r2) {
			return nil, errors.New("model NAN")
		}
		result[key] = &LinearModel{
			Model: model,
			MAE:   mae,
			MSE:   mse,
			RMSE:  rmse,
			R2:    r2,
		}
	}
	return result, nil
}

// calculateMAE calculate MAE, MSE, RMSE, R² of model
func (strategy *training) evaluate(out base.FixedDataGrid, label base.FixedDataGrid) (float64, float64, float64, float64, error) {
	attrSpec1, _ := label.GetAttribute(label.AllAttributes()[len(label.AllAttributes())-1])
	attrSpec2, _ := out.GetAttribute(out.AllAttributes()[0])
	_, length := out.Size()
	maeSum := 0.0
	mseSum := 0.0
	mean := 0.0
	tssSum := 0.0
	for i := 0; i < length; i++ {
		maeSum += math.Abs(base.UnpackBytesToFloat(label.Get(attrSpec1, i)) - base.UnpackBytesToFloat(out.Get(attrSpec2, i)))
		mseSum += math.Pow(base.UnpackBytesToFloat(label.Get(attrSpec1, i))-base.UnpackBytesToFloat(out.Get(attrSpec2, i)), 2)
		mean += base.UnpackBytesToFloat(label.Get(attrSpec1, i))
	}
	mean = mean / float64(length)
	for i := 0; i < length; i++ {
		tssSum += math.Pow(base.UnpackBytesToFloat(label.Get(attrSpec1, i))-mean, 2)
	}
	return maeSum / float64(length), mseSum / float64(length), math.Sqrt(mseSum / float64(length)), 1 - mseSum/tssSum, nil
}

// loadRecord read record from file and transform it to instance.
func (strategy *training) loadRecord(reader *bufio.Reader) (*base.DenseInstances, error) {
	if strategy.currentRecordLine < strategy.maxRecordLine {
		strategy.buffer = ""
		for i := 0; i < strategy.maxBufferLine; i++ {
			readString, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return nil, err
			}
			if len(readString) == 0 {
				break
			}
			strategy.buffer += readString
			strategy.currentRecordLine += 1
		}
		if strategy.buffer == "" {
			return nil, errors.New("file empty")
		}
		strReader := strings.NewReader(strategy.buffer)
		instance, err := base.ParseCSVToInstancesFromReader(strReader, false)
		if err != nil {
			return nil, err
		}
		return instance, nil
	}
	return nil, nil
}

// missingValue return effective rows in order to droping missing value.
func (strategy *training) missingValue(instances *base.DenseInstances) ([]int, error) {
	cal, row := instances.Size()
	attributes := instances.AllAttributes()
	effectiveRow := make([]int, 0)
	for i := 0; i < row; i++ {
		drop := false
		for j := 0; j < cal; j++ {
			attrSpec, _ := instances.GetAttribute(attributes[j])
			if base.UnpackBytesToFloat(instances.Get(attrSpec, i)) == -1 {
				drop = true
				break
			}
		}
		if !drop {
			effectiveRow = append(effectiveRow, i)
		}
	}
	return effectiveRow, nil
}

// normalize using z-score or max_min normalization to normalize float64 filed.
func (strategy *training) normalize(instance *base.DenseInstances, rows []int, Zscore bool) error {
	attributes := instance.AllAttributes()
	if Zscore {
		meanValue := make([]float64, NormalizedFieldNum)
		stdValue := make([]float64, NormalizedFieldNum)
		for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
			attrSpec, _ := instance.GetAttribute(attributes[i])
			for j := 0; j < len(rows); j++ {
				x := base.UnpackBytesToFloat(instance.Get(attrSpec, rows[j]))
				meanValue[i+NormalizedFieldNum-len(attributes)] += x
			}
		}
		for i := 0; i < NormalizedFieldNum; i++ {
			meanValue[i] = meanValue[i] / float64(len(rows))
		}

		for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
			attrSpec, _ := instance.GetAttribute(attributes[i])
			for j := 0; j < len(rows); j++ {
				x := base.UnpackBytesToFloat(instance.Get(attrSpec, rows[j]))
				stdValue[i+NormalizedFieldNum-len(attributes)] += math.Pow(x-meanValue[i+NormalizedFieldNum-len(attributes)], 2)
			}
		}
		for i := 0; i < NormalizedFieldNum; i++ {
			stdValue[i] = math.Sqrt(stdValue[i] / meanValue[i])
			if stdValue[i] == 0 {
				stdValue[i] = 1
			}
		}

		for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
			attrSpec, _ := instance.GetAttribute(attributes[i])
			for j := 0; j < len(rows); j++ {
				x := base.UnpackBytesToFloat(instance.Get(attrSpec, rows[j]))
				bytes := base.PackFloatToBytes((x - meanValue[i+NormalizedFieldNum-len(attributes)]) / stdValue[i+NormalizedFieldNum-len(attributes)])
				instance.Set(attrSpec, rows[j], bytes)
			}
		}
		return nil
	}
	maxValue := make([]float64, NormalizedFieldNum)
	minValue := make([]float64, NormalizedFieldNum)
	for i := 0; i < NormalizedFieldNum; i++ {
		minValue[i] = math.MaxFloat64
	}
	for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
		attrSpec, _ := instance.GetAttribute(attributes[i])
		for j := 0; j < len(rows); j++ {
			x := base.UnpackBytesToFloat(instance.Get(attrSpec, rows[j]))
			if x > maxValue[i+NormalizedFieldNum-len(attributes)] {
				maxValue[i+NormalizedFieldNum-len(attributes)] = x
			}
			if x < minValue[i+NormalizedFieldNum-len(attributes)] {
				minValue[i+NormalizedFieldNum-len(attributes)] = x
			}
		}
	}
	for i := 0; i < NormalizedFieldNum; i++ {
		maxValue[i] = maxValue[i] - minValue[i]
	}
	for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
		attrSpec, _ := instance.GetAttribute(attributes[i])
		for j := 0; j < len(rows); j++ {
			x := base.UnpackBytesToFloat(instance.Get(attrSpec, rows[j]))
			bytes := base.PackFloatToBytes((x - minValue[i+NormalizedFieldNum-len(attributes)]) / maxValue[i+NormalizedFieldNum-len(attributes)])
			instance.Set(attrSpec, rows[j], bytes)
		}
	}
	return nil
}

// split divide dataset by ParentID
func (strategy *training) split(instance *base.DenseInstances, rows []int, result map[float64]*base.DenseInstances) error {
	attributes := instance.AllAttributes()
	attrSpec, err := instance.GetAttribute(attributes[1])
	if err != nil {
		return err
	}
	for i := 0; i < len(rows); i++ {
		ID := base.UnpackBytesToFloat(instance.Get(attrSpec, rows[i]))
		if _, ok := result[ID]; !ok {
			result[ID] = base.NewDenseInstances()
			for j := 2; j < len(attributes); j++ {
				if j == len(attributes)-1 {
					label := base.NewFloatAttribute("label")
					result[ID].AddAttribute(label)
					err := result[ID].AddClassAttribute(label)
					if err != nil {
						return err
					}
				} else {
					result[ID].AddAttribute(base.NewFloatAttribute("float" + strconv.Itoa(j-2)))
				}
			}
		}
		_, length := result[ID].Size()
		err := result[ID].Extend(1)
		if err != nil {
			return err
		}
		for j := 2; j < len(attributes); j++ {
			attrSp, err := instance.GetAttribute(attributes[j])
			x := instance.Get(attrSp, rows[i])
			if err != nil {
				return err
			}
			attrSpt, _ := result[ID].GetAttribute(result[ID].AllAttributes()[j-2])
			result[ID].Set(attrSpt, length, x)
		}
	}
	return nil
}
