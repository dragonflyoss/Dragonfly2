package models

import (
	"errors"
	"math/rand"

	"github.com/sjwhitworth/golearn/base"
)

// LinearRegression linear regression model struct.
type LinearRegression struct {
	fitted                 bool
	Disturbance            float64
	RegressionCoefficients []float64
	attrs                  []base.Attribute
	cls                    base.Attribute
}

// NewLinearRegression return an instance of linear regression model.
func NewLinearRegression() *LinearRegression {
	return &LinearRegression{fitted: false}
}

// Fit train parameters of model to fit the data provided.
func (lr *LinearRegression) Fit(inst base.FixedDataGrid, learningRate float64) error {
	_, rows := inst.Size()

	classAttrs := inst.AllClassAttributes()
	if len(classAttrs) != 1 {
		return errors.New("only 1 class variable is permitted")
	}
	classAttrSpecs := base.ResolveAttributes(inst, classAttrs)

	allAttrs := base.NonClassAttributes(inst)
	attrs := make([]base.Attribute, 0)
	for _, a := range allAttrs {
		if _, ok := a.(*base.FloatAttribute); ok {
			attrs = append(attrs, a)
		}
	}
	attrSpecs := base.ResolveAttributes(inst, attrs)

	cols := len(attrs) + 1
	regressionCoefficients := make([]float64, cols)
	for i := 0; i < cols; i++ {
		regressionCoefficients[i] = rand.Float64()
	}

	for i := 0; i < rows; i++ {
		out := regressionCoefficients[0]
		for j := 1; j < cols; j++ {
			out += base.UnpackBytesToFloat(inst.Get(attrSpecs[j-1], i)) * regressionCoefficients[j]
		}
		for j := 1; j < cols; j++ {
			regressionCoefficients[j] += learningRate * (base.UnpackBytesToFloat(inst.Get(classAttrSpecs[0], i)) - out) * base.UnpackBytesToFloat(inst.Get(attrSpecs[j-1], i))
		}
	}

	lr.Disturbance = regressionCoefficients[0]
	lr.RegressionCoefficients = regressionCoefficients[1:]
	lr.fitted = true
	lr.attrs = attrs
	lr.cls = classAttrs[0]
	return nil
}

// Predict use parameters of model to predict the data provided.
func (lr *LinearRegression) Predict(X base.FixedDataGrid) (base.FixedDataGrid, error) {
	if !lr.fitted {
		return nil, errors.New("no fitted model")
	}

	ret := base.GeneratePredictionVector(X)
	attrSpecs := base.ResolveAttributes(X, lr.attrs)
	clsSpec, err := ret.GetAttribute(lr.cls)
	if err != nil {
		return nil, err
	}

	err = X.MapOverRows(attrSpecs, func(row [][]byte, i int) (bool, error) {
		var prediction = lr.Disturbance
		for j, r := range row {
			prediction += base.UnpackBytesToFloat(r) * lr.RegressionCoefficients[j]
		}

		ret.Set(clsSpec, i, base.PackFloatToBytes(prediction))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return ret, nil
}
