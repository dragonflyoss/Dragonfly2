package training

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"

	"github.com/sjwhitworth/golearn/base"
)

// Normalize using z-score or max_min normalization to normalize float64 filed.
func Normalize(instance *base.DenseInstances, Zscore bool) error {
	_, row := instance.Size()
	attributes := instance.AllAttributes()
	if Zscore {
		meanValue := make([]float64, NormalizedFieldNum)
		stdValue := make([]float64, NormalizedFieldNum)
		for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
			attrSpec, _ := instance.GetAttribute(attributes[i])
			for j := 0; j < row; j++ {
				x := base.UnpackBytesToFloat(instance.Get(attrSpec, j))
				meanValue[i+NormalizedFieldNum-len(attributes)] += x
			}
		}
		for i := 0; i < NormalizedFieldNum; i++ {
			meanValue[i] = meanValue[i] / float64(row)
		}

		for i := len(attributes) - NormalizedFieldNum; i < len(attributes); i++ {
			attrSpec, _ := instance.GetAttribute(attributes[i])
			for j := 0; j < row; j++ {
				x := base.UnpackBytesToFloat(instance.Get(attrSpec, j))
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
			for j := 0; j < row; j++ {
				x := base.UnpackBytesToFloat(instance.Get(attrSpec, j))
				bytes := base.PackFloatToBytes((x - meanValue[i+NormalizedFieldNum-len(attributes)]) / stdValue[i+NormalizedFieldNum-len(attributes)])
				instance.Set(attrSpec, j, bytes)
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
		for j := 0; j < row; j++ {
			x := base.UnpackBytesToFloat(instance.Get(attrSpec, j))
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
		for j := 0; j < row; j++ {
			x := base.UnpackBytesToFloat(instance.Get(attrSpec, j))
			bytes := base.PackFloatToBytes((x - minValue[i+NormalizedFieldNum-len(attributes)]) / maxValue[i+NormalizedFieldNum-len(attributes)])
			instance.Set(attrSpec, j, bytes)
		}
	}
	return nil
}

// MissingValue use effective data to replace missing data.
func MissingValue(instances *base.DenseInstances) error {
	cal, row := instances.Size()
	attributes := instances.AllAttributes()
	effectiveData := make([]float64, cal)
	for i := 0; i < cal; i++ {
		effectiveData[i] = -1
	}
	counter := 0
	for i := 0; i < row; i++ {
		for j := 0; j < cal; j++ {
			attrSpec, _ := instances.GetAttribute(attributes[j])
			x := base.UnpackBytesToFloat(instances.Get(attrSpec, i))
			if x != -1 && effectiveData[j] == -1 {
				effectiveData[j] = x
				counter += 1
			}
		}
		if counter == cal-1 {
			break
		}
	}

	for i := 0; i < row; i++ {
		for j := 0; j < cal; j++ {
			attrSpec, _ := instances.GetAttribute(attributes[j])
			if base.UnpackBytesToFloat(instances.Get(attrSpec, i)) == -1 {
				instances.Set(attrSpec, i, base.PackFloatToBytes(effectiveData[j]))
			}
		}
	}
	return nil
}

// LoadRecord read record from file and transform it to instance.
func LoadRecord(reader io.ReadCloser, loopTimes int) (*base.DenseInstances, error) {
	r := bufio.NewReader(reader)
	buf := new(bytes.Buffer)

	for i := 0; i < loopTimes; i++ {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}

		if err == io.EOF {
			break
		}
		// TODO how to handle
		if i == 0 {
			continue
		}
		buf.Write([]byte(line))
	}
	if buf.Len() == 0 {
		return nil, errors.New("file empty")
	}
	strReader := bytes.NewReader(buf.Bytes())
	instance, err := base.ParseCSVToInstancesFromReader(strReader, false)
	if err != nil {
		return nil, err
	}
	return instance, nil
}
