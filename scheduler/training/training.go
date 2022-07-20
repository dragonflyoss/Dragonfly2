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
	"github.com/sjwhitworth/golearn/base"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const (
	// NormalizedFieldNum field which needs normalization lies at the end of record.
	NormalizedFieldNum = 7

	// DefaultMaxBufferLine capacity of lines which read from record file.
	DefaultMaxBufferLine = 64

	// DefaultMaxRecordLine capacity of lines which local memory obtains.
	DefaultMaxRecordLine = 100000
)

const (
	// DefaultFileName filename which stores records.
	DefaultFileName = "record.csv"
)

// Training is the interface used for train models.
type Training interface {
	// PreProcess load and clean data before training.
	PreProcess() (*base.DenseInstances, error)
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
}

// New return a Training instance.
func New(baseDir string) (Training, error) {
	t := &training{
		baseDir:       baseDir,
		fileName:      filepath.Join(baseDir, DefaultFileName),
		maxBufferLine: DefaultMaxBufferLine,
		maxRecordLine: DefaultMaxRecordLine,
	}

	file, err := os.Open(t.fileName)
	if err != nil {
		return nil, err
	}
	file.Close()

	return t, nil
}

// PreProcess load and clean data before training.
func (strategy *training) PreProcess() (*base.DenseInstances, error) {
	// TODO using pipeline, load -> missing -> normalize -> split
	f, _ := os.Open(strategy.fileName)
	instance, err := strategy.loadRecord(bufio.NewReader(f))
	f.Close()
	return instance, err
}

// loadRecord read record from file and transform it to instance.
func (strategy *training) loadRecord(reader *bufio.Reader) (*base.DenseInstances, error) {
	if strategy.currentRecordLine < strategy.maxRecordLine {
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

// normalize using max min normalization to normalize float64 filed.
func (strategy *training) normalize(instance *base.DenseInstances, rows []int) error {
	attributes := instance.AllAttributes()
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
