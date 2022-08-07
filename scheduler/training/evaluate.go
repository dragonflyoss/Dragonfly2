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
}

// Evaluate calculateMAE calculate MAE, MSE, RMSE, R² of model.
func Evaluate(out base.FixedDataGrid, label base.FixedDataGrid) (*Eval, error) {
	attrSpec1, _ := label.GetAttribute(label.AllAttributes()[len(label.AllAttributes())-1])
	attrSpec2, _ := out.GetAttribute(out.AllAttributes()[0])
	_, length := out.Size()
	maeSum, mseSum, mean, tssSum := InitSum, InitSum, InitSum, InitSum
	for i := 0; i < length; i++ {
		maeSum += math.Abs(base.UnpackBytesToFloat(label.Get(attrSpec1, i)) - base.UnpackBytesToFloat(out.Get(attrSpec2, i)))
		mseSum += math.Pow(base.UnpackBytesToFloat(label.Get(attrSpec1, i))-base.UnpackBytesToFloat(out.Get(attrSpec2, i)), 2)
		mean += base.UnpackBytesToFloat(label.Get(attrSpec1, i))
	}
	mean = mean / float64(length)
	for i := 0; i < length; i++ {
		tssSum += math.Pow(base.UnpackBytesToFloat(label.Get(attrSpec1, i))-mean, 2)
	}
	return &Eval{
		MAE:  maeSum / float64(length),
		MSE:  mseSum / float64(length),
		RMSE: math.Sqrt(mseSum / float64(length)),
		R2:   1 - mseSum/tssSum,
	}, nil
}

func (e *Eval) CheckEval() error {
	if math.IsNaN(e.MAE) || math.IsNaN(e.MSE) || math.IsNaN(e.RMSE) || math.IsNaN(e.R2) {
		return errors.New("model NAN")
	}
	return nil
}
