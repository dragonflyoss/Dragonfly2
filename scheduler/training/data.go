package training

import (
	"io"

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
