package training

import (
	"math"
	"strconv"

	"github.com/sjwhitworth/golearn/base"
)

// Split divide dataset by ParentID.
func Split(instance *base.DenseInstances, rows []int, result map[float64]*base.DenseInstances) error {
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

// Normalize using z-score or max_min normalization to normalize float64 filed.
func Normalize(instance *base.DenseInstances, rows []int, Zscore bool) error {
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

// MissingValue return effective rows in order to droping missing value.
func MissingValue(instances *base.DenseInstances) ([]int, error) {
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
