package models

import (
	"encoding/json"
	"errors"
	"math/rand"

	"github.com/mitchellh/mapstructure"
	"github.com/sjwhitworth/golearn/base"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

// LinearRegression linear regression model struct.
type LinearRegression struct {
	Fitted                 bool                   `json:"fitted"`
	Disturbance            float64                `json:"disturbance"`
	RegressionCoefficients []float64              `json:"regression_coefficients"`
	Attrs                  []*base.FloatAttribute `json:"attrs"`
	Cls                    *base.FloatAttribute   `json:"cls"`
}

// NewLinearRegression return an instance of linear regression model.
func NewLinearRegression() *LinearRegression {
	return &LinearRegression{Fitted: false}
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
	lr.Fitted = true
	lr.Attrs = make([]*base.FloatAttribute, len(attrs))
	for idx, a := range attrs {
		lr.Attrs[idx] = a.(*base.FloatAttribute)
	}
	lr.Cls = classAttrs[0].(*base.FloatAttribute)
	return nil
}

// Predict use parameters of model to predict the data provided.
func (lr *LinearRegression) Predict(X base.FixedDataGrid) (base.FixedDataGrid, error) {
	if !lr.Fitted {
		logger.Info("no fitted model")
		return nil, errors.New("no fitted model")
	}

	ret := base.GeneratePredictionVector(X)
	attrs := make([]base.Attribute, len(lr.Attrs))
	for idx, a := range lr.Attrs {
		attrs[idx] = a
	}
	attrSpecs := base.ResolveAttributes(X, attrs)
	clsSpec, err := ret.GetAttribute(lr.Cls)
	if err != nil {
		logger.Infof("LinearRegression error happens, error is %v", err)
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
		logger.Infof("LinearRegression error happens, error is %v", err)
		return nil, err
	}
	return ret, nil
}

func (lr *LinearRegression) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"fitted":                  lr.Fitted,
		"disturbance":             lr.Disturbance,
		"regression_coefficients": lr.RegressionCoefficients,
		"attrs":                   lr.marshalFloatAttributes(),
		"cls":                     marshalFloatAttribute(lr.Cls),
	})
}

func marshalFloatAttribute(f *base.FloatAttribute) map[string]interface{} {
	return map[string]interface{}{
		"name":      f.Name,
		"precision": f.Precision,
	}
}

func (lr *LinearRegression) marshalFloatAttributes() []map[string]interface{} {
	ans := make([]map[string]interface{}, len(lr.Attrs))
	for idx, attr := range lr.Attrs {
		ans[idx] = marshalFloatAttribute(attr)
	}
	return ans
}

func (lr *LinearRegression) UnmarshalJSON(data []byte) error {
	var d map[string]interface{}
	err := json.Unmarshal(data, &d)
	if err != nil {
		return err
	}

	err = mapstructure.Decode(d, lr)
	if err != nil {
		return err
	}
	val, ok := d["regression_coefficients"]
	if ok {
		var coefficients []float64
		err = mapstructure.Decode(val, &coefficients)
		if err != nil {
			return err
		}
		lr.RegressionCoefficients = coefficients
	}
	return nil
}
