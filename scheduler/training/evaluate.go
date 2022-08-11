package training

import (
	"errors"
	"math"

	"github.com/sjwhitworth/golearn/base"
)

type Eval struct {
	// MAE mean absolute error.
	MAE float64

	// MSE mean square error.
	MSE float64

	// RMSE Root Mean Square Error.
	RMSE float64

	// R² coefficient of determination.
	R2 float64

	outArr []float64

	labelArr []float64
}

func (e *Eval) EvaluateStore(out base.FixedDataGrid, label base.FixedDataGrid) {
	attrSpec1, _ := label.GetAttribute(label.AllAttributes()[len(label.AllAttributes())-1])
	attrSpec2, _ := out.GetAttribute(out.AllAttributes()[0])
	_, length := out.Size()
	for i := 0; i < length; i++ {
		e.outArr = append(e.outArr, base.UnpackBytesToFloat(out.Get(attrSpec2, i)))
		e.labelArr = append(e.labelArr, base.UnpackBytesToFloat(label.Get(attrSpec1, i)))
	}
	return
}

func (e *Eval) EvaluateCal() error {
	maeSum, mseSum, mean, tssSum := InitSum, InitSum, InitSum, InitSum
	for i := 0; i < len(e.outArr); i++ {
		maeSum += math.Abs(e.labelArr[i] - e.outArr[i])
		mseSum += math.Pow(e.labelArr[i]-e.outArr[i], 2)
		mean += e.labelArr[i]
	}
	mean = mean / float64(len(e.outArr))
	for i := 0; i < len(e.outArr); i++ {
		tssSum += math.Pow(e.labelArr[i]-mean, 2)
	}
	e.MAE = maeSum / float64(len(e.outArr))
	e.MSE = mseSum / float64(len(e.outArr))
	e.RMSE = math.Sqrt(mseSum / float64(len(e.outArr)))
	e.R2 = 1 - mseSum/tssSum
	err := e.CheckEval()
	if err != nil {
		return err
	}
	return nil
}

func (e *Eval) CheckEval() error {
	if math.IsNaN(e.MAE) || math.IsNaN(e.MSE) || math.IsNaN(e.RMSE) || math.IsNaN(e.R2) {
		return errors.New("model NAN")
	}
	return nil
}

func NewEval() *Eval {
	return &Eval{
		outArr:   make([]float64, 0),
		labelArr: make([]float64, 0),
	}
}
