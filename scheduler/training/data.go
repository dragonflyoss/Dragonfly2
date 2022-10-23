package training

import (
	"io"
	"math"

	"github.com/sjwhitworth/golearn/base"
)

// Data Train provides training functions.
type Data struct {
	Reader io.ReadCloser
	// currentRecordLine capacity of lines which training has.
	TotalDataRecordLine int64
	TotalTestRecordLine int64
	Options             *DataOptions
}

type DataOptions struct {
	// maxBufferLine capacity of lines which reads from record once.
	MaxBufferLine int

	// maxRecordLine capacity of lines which local memory obtains.
	MaxRecordLine int

	TestPercent float64
}

type DataOptionFunc func(options *DataOptions)

func WithMaxBufferLine(MaxBufferLine int) DataOptionFunc {
	return func(options *DataOptions) {
		options.MaxBufferLine = MaxBufferLine
	}
}

func WithMaxRecordLine(MaxRecordLine int) DataOptionFunc {
	return func(options *DataOptions) {
		options.MaxRecordLine = MaxRecordLine
	}
}

func WithTestPercent(TestPercent float64) DataOptionFunc {
	return func(options *DataOptions) {
		options.TestPercent = TestPercent
	}
}

// New return a Training instance.
func New(reader io.ReadCloser, option ...DataOptionFunc) (*Data, error) {
	t := &Data{
		Reader: reader,
		Options: &DataOptions{
			MaxBufferLine: DefaultMaxBufferLine,
			MaxRecordLine: DefaultMaxRecordLine,
			TestPercent:   TestSetPercent,
		},
	}
	for _, o := range option {
		o(t.Options)
	}

	return t, nil
}

func (d *Data) MinData(maxRecord int, totalRecord int64) int {
	if int64(maxRecord) > totalRecord {
		d.TotalDataRecordLine = 0
		return int(totalRecord)
	}
	d.TotalTestRecordLine -= int64(maxRecord)
	return maxRecord
}

func (d *Data) MinTest(maxRecord int, totalRecord int64) int {
	if int64(maxRecord) > totalRecord {
		d.TotalTestRecordLine = 0
		return int(totalRecord)
	}
	d.TotalTestRecordLine -= int64(maxRecord)
	return maxRecord
}

// PreProcess load and clean data before training.
func (d *Data) PreProcess(loadType string) (*base.DenseInstances, error) {
	loopTimes := 0
	switch loadType {
	case LoadData:
		loopTimes = d.MinData(d.Options.MaxRecordLine, d.TotalDataRecordLine)
	case LoadTest:
		loopTimes = d.MinTest(d.Options.MaxRecordLine, d.TotalTestRecordLine)
	}
	instance, err := LoadRecord(d.Reader, loopTimes)
	if err != nil {
		return nil, err
	}

	err = MissingValue(instance)
	if err != nil {
		return nil, err
	}
	err = Normalize(instance, false)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func PredictNormalize(instance *base.DenseInstances) error {
	_, row := instance.Size()
	attributes := instance.AllAttributes()
	maxValue := make([]float64, NormalizedFieldNum)
	minValue := make([]float64, NormalizedFieldNum)
	for i := 0; i < NormalizedFieldNum; i++ {
		minValue[i] = math.MaxFloat64
	}
	for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
		attrSpec, _ := instance.GetAttribute(attributes[i])
		for j := 0; j < row; j++ {
			x := base.UnpackBytesToFloat(instance.Get(attrSpec, j))
			minValue[i+NormalizedFieldNum-len(attributes)] = x - AverageFieldSigma
			maxValue[i+NormalizedFieldNum-len(attributes)] = x + AverageFieldSigma
		}
	}
	for i := 0; i < NormalizedFieldNum; i++ {
		maxValue[i] = maxValue[i] - minValue[i]
		if maxValue[i] == 0 {
			maxValue[i] = 1
		}
	}
	for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
		attrSpec, _ := instance.GetAttribute(attributes[i])
		for j := 0; j < row; j++ {
			x := base.UnpackBytesToFloat(instance.Get(attrSpec, j))
			bytes := base.PackFloatToBytes((x - minValue[i+NormalizedFieldNum-len(attributes)]) / maxValue[i+NormalizedFieldNum-len(attributes)])
			instance.Set(attrSpec, j, bytes)
		}
	}
	return nil
}
