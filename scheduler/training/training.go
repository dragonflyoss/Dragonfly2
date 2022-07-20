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
	"github.com/sjwhitworth/golearn/base"
	"github.com/sjwhitworth/golearn/linear_models"
	"math"
	"strconv"
)

const (
	// TIMEBUCKETGAP length of gap in time bucket.
	TIMEBUCKETGAP = 7200

	// INTFIELDNUM number of int field.
	INTFIELDNUM = 14

	// FLOATFIELDNUM number of float field.
	FLOATFIELDNUM = 2

	// STRFIELDNUM number of string field.
	STRFIELDNUM = 2

	// TARGETFIELDNUM number of target field.
	TARGETFIELDNUM = 1

	// TESTPERCENT percent of test data.
	TESTPERCENT = 0.2

	// MSETHRESHOLD threshold of MSE.
	MSETHRESHOLD = 100
)

type TempData struct {
	// id is the signal of peer.
	id string

	// ip is 0 if peer ip equals parent ip else 1
	ip uint64

	// bizTag is 0 if peer bizTag equals parent bizTag else 1
	bizTag uint64

	// hostName is 0 if peer hostName equals parent hostName else 1
	hostName uint64

	// securityDomain is 0 if peer securityDomain equals parent securityDomain else 1
	securityDomain uint64

	// hostType uint64
	hostType uint64

	// idc is 0 if peer idc equals parent idc else 1
	idc uint64

	// netTopology is 0 if peer netTopology equals parent netTopology else 1
	netTopology uint64

	// location is 0 if peer location equals parent location else 1
	location uint64

	// parentId is the signal of parent
	parentId string

	// state uint64
	state uint64

	// rate：contentLength / cost
	rate float64

	// parentPiece：totalPieceCount / parentPieceCount
	parentPiece float64

	// parentHostType uint64
	parentHostType uint64

	// uploadRate：freeUploadLoad / parentFreeUploadLoad
	uploadRate float64

	// createAt peer create time parsed by TimeBucket
	createAt uint64

	// updateAt peer update time parsed by TimeBucket
	updateAt uint64

	// parentCreateAt parent create time parsed by TimeBucket
	parentCreateAt uint64

	// parentUpdateAt parent update time parsed by TimeBucket
	parentUpdateAt uint64
}

type Data struct {
	intArray []uint64

	floatArray []float64

	stringArray []string

	targetArray []float64
}

// NewData construct of Data
func NewData(temp *TempData) *Data {
	var data Data
	data.intArray = make([]uint64, INTFIELDNUM)
	data.intArray[0] = temp.ip
	data.intArray[1] = temp.bizTag
	data.intArray[2] = temp.hostName
	data.intArray[3] = temp.securityDomain
	data.intArray[4] = temp.hostType
	data.intArray[5] = temp.idc
	data.intArray[6] = temp.netTopology
	data.intArray[7] = temp.location
	data.intArray[8] = temp.state
	data.intArray[9] = temp.parentHostType
	data.intArray[10] = temp.createAt
	data.intArray[11] = temp.updateAt
	data.intArray[12] = temp.parentCreateAt
	data.intArray[13] = temp.parentUpdateAt
	data.floatArray = make([]float64, FLOATFIELDNUM)
	data.floatArray[0] = temp.uploadRate
	data.floatArray[1] = temp.parentPiece
	data.stringArray = make([]string, STRFIELDNUM)
	data.stringArray[0] = temp.id
	data.stringArray[1] = temp.parentId
	data.targetArray = make([]float64, TARGETFIELDNUM)
	data.targetArray[0] = temp.rate
	return &data
}

// StringRecordCompare return 1 if str1 == str2, else 0.
func StringRecordCompare(str1 string, str2 string) uint64 {
	if str1 == str2 {
		return 0
	}
	return 1
}

// TimeBucket divide timestamp with TIMEBUCKETGAP.
func TimeBucket(time int64) uint64 {
	return uint64(time / TIMEBUCKETGAP)
}

// RecordTransData preprocess of machine learning, record to data struct.
func RecordTransData(record *storage.Record) *Data {
	data := &TempData{
		id:             record.ID,
		ip:             StringRecordCompare(record.IP, record.ParentIP),
		bizTag:         StringRecordCompare(record.BizTag, record.ParentBizTag),
		hostName:       StringRecordCompare(record.Hostname, record.ParentHostname),
		securityDomain: StringRecordCompare(record.SecurityDomain, record.ParentSecurityDomain),
		hostType:       uint64(record.HostType),
		idc:            StringRecordCompare(record.IDC, record.ParentIDC),
		netTopology:    StringRecordCompare(record.NetTopology, record.ParentNetTopology),
		location:       StringRecordCompare(record.Location, record.ParentLocation),
		parentId:       record.ParentID,
		state:          uint64(record.State),
		rate:           float64(record.ContentLength) / float64(record.Cost),
		// TODO pieceCount redundancy
		parentPiece:    float64(record.TotalPieceCount) / float64(record.ParentPieceCount),
		parentHostType: uint64(record.ParentHostType),
		uploadRate:     float64(record.FreeUploadLoad) / float64(record.ParentFreeUploadLoad),
		createAt:       TimeBucket(record.CreateAt),
		updateAt:       TimeBucket(record.UpdateAt),
		parentCreateAt: TimeBucket(record.ParentCreateAt),
		parentUpdateAt: TimeBucket(record.ParentUpdateAt),
	}
	return NewData(data)
}

// RecordsTransData batch process, records to data.
func RecordsTransData(records []*storage.Record) []*Data {
	data := make([]*Data, len(records))
	for i, each := range records {
		data[i] = RecordTransData(each)
	}
	return data
}

// GetInstanceHeaders return an instance with headers and its attr list.
func GetInstanceHeaders() (*base.DenseInstances, []base.AttributeSpec, error) {
	instance := base.NewDenseInstances()

	attrList := make([]base.AttributeSpec, INTFIELDNUM+FLOATFIELDNUM+TARGETFIELDNUM)
	for i := 0; i < INTFIELDNUM; i++ {
		attrList[i] = instance.AddAttribute(base.NewFloatAttribute("int" + strconv.Itoa(i)))
	}
	for i := INTFIELDNUM; i < INTFIELDNUM+FLOATFIELDNUM; i++ {
		attrList[i] = instance.AddAttribute(base.NewFloatAttribute("float" + strconv.Itoa(i)))
	}
	for i := INTFIELDNUM + FLOATFIELDNUM; i < INTFIELDNUM+FLOATFIELDNUM+TARGETFIELDNUM; i++ {
		attr := base.NewFloatAttribute("target" + strconv.Itoa(i))
		attrList[i] = instance.AddAttribute(attr)
		err := instance.AddClassAttribute(attr)
		if err != nil {
			return nil, nil, err
		}
	}
	return instance, attrList, nil
}

// SplitByParentID split data by parentID
func SplitByParentID(data []*Data) map[string][]*Data {
	result := make(map[string][]*Data, 0)
	for _, each := range data {
		if _, ok := result[each.stringArray[1]]; !ok {
			result[each.stringArray[1]] = make([]*Data, 0)
		} else {
			result[each.stringArray[1]] = append(result[each.stringArray[1]], each)
		}
	}
	return result
}

// MaxMinNormalize return data which contains the (max - min) value and min value of data.
func MaxMinNormalize(data []*Data) (*Data, *Data) {
	var maxData Data
	var minData Data
	maxData.floatArray = make([]float64, FLOATFIELDNUM)
	maxData.targetArray = make([]float64, TARGETFIELDNUM)
	minData.floatArray = make([]float64, FLOATFIELDNUM)
	minData.targetArray = make([]float64, TARGETFIELDNUM)
	for j := 0; j < FLOATFIELDNUM; j++ {
		minData.floatArray[j] = math.MaxFloat64
	}
	for j := 0; j < TARGETFIELDNUM; j++ {
		minData.targetArray[j] = math.MaxFloat64
	}
	for i := 0; i < len(data); i++ {
		for j := 0; j < len(data[i].floatArray); j++ {
			if data[i].floatArray[j] > maxData.floatArray[j] {
				maxData.floatArray[j] = data[i].floatArray[j]
			}
			if data[i].floatArray[j] < minData.floatArray[j] {
				minData.floatArray[j] = data[i].floatArray[j]
			}
		}
		for j := 0; j < len(data[i].targetArray); j++ {
			if data[i].targetArray[j] > maxData.targetArray[j] {
				maxData.targetArray[j] = data[i].targetArray[j]
			}
			if data[i].targetArray[j] < minData.targetArray[j] {
				minData.targetArray[j] = data[i].targetArray[j]
			}
		}
	}
	for j := 0; j < FLOATFIELDNUM; j++ {
		maxData.floatArray[j] = maxData.floatArray[j] - minData.floatArray[j]
	}
	for j := 0; j < TARGETFIELDNUM; j++ {
		maxData.targetArray[j] = maxData.targetArray[j] - minData.targetArray[j]
	}
	return &maxData, &minData
}

// DataToInstances preprocess of machine learning, data to instance.
func DataToInstances(data []*Data) (*base.DenseInstances, error) {
	instance, attrList, _ := GetInstanceHeaders()
	err := instance.Extend(len(data))
	if err != nil {
		return nil, err
	}
	maxData, minData := MaxMinNormalize(data)
	for i := 0; i < len(data); i++ {
		for j := 0; j < INTFIELDNUM; j++ {
			instance.Set(attrList[j], i, base.PackFloatToBytes(float64(data[i].intArray[j])))
		}
		for j := INTFIELDNUM; j < INTFIELDNUM+FLOATFIELDNUM; j++ {
			x := (data[i].floatArray[j-INTFIELDNUM] - minData.floatArray[j-INTFIELDNUM]) / maxData.floatArray[j-INTFIELDNUM]
			instance.Set(attrList[j], i, base.PackFloatToBytes(x))
		}
		for j := INTFIELDNUM + FLOATFIELDNUM; j < INTFIELDNUM+FLOATFIELDNUM+TARGETFIELDNUM; j++ {
			x := (data[i].targetArray[j-INTFIELDNUM-FLOATFIELDNUM] - minData.targetArray[j-INTFIELDNUM-FLOATFIELDNUM]) / maxData.targetArray[j-INTFIELDNUM-FLOATFIELDNUM]
			instance.Set(attrList[j], i, base.PackFloatToBytes(x))
		}
	}
	return instance, nil
}

// train return a model of linearRegression trained by instance.
func train(instance base.FixedDataGrid) (*linear_models.LinearRegression, error) {
	lr := linear_models.NewLinearRegression()
	err := lr.Fit(instance)
	if err != nil {
		return nil, err
	}
	return lr, nil
}

// evaluate return MSE and length of data.
func evaluate(instance base.FixedDataGrid, lr *linear_models.LinearRegression) (float64, int, error) {
	out, err := lr.Predict(instance)
	if err != nil {
		return -1, -1, err
	}
	_, length := out.Size()
	attrSpec, err := out.GetAttribute(out.AllAttributes()[0])
	if err != nil {
		return -1, -1, err
	}
	sum := 0.0
	for i := 0; i < length; i++ {
		x := base.UnpackBytesToFloat(out.Get(attrSpec, i)) - base.UnpackBytesToFloat(instance.Get(attrSpec, i))
		sum += math.Pow(x, 2)
	}
	return sum, length, nil
}

type Training struct {
}

// Serve return models map and MSE value of evaluation.
func (strategy *Training) Serve(records []*storage.Record) (map[string]*linear_models.LinearRegression, float64, error) {
	data := RecordsTransData(records)
	parentIDMap := SplitByParentID(data)
	resultMap := make(map[string]*linear_models.LinearRegression, 0)
	mse_sum := 0.0
	length_sum := 0
	for key, value := range parentIDMap {
		instance, err := DataToInstances(value)
		if err != nil {
			return nil, -1, err
		}
		trainData, testData := base.InstancesTrainTestSplit(instance, TESTPERCENT)
		lr, err := train(trainData)
		if err != nil {
			return nil, -1, err
		}
		mse, length, err := evaluate(testData, lr)
		mse_sum += mse
		length_sum += length
		if err != nil {
			return nil, -1, err
		}
		resultMap[key] = lr
	}

	return resultMap, mse_sum / float64(length_sum), nil
}
