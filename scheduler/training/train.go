package training

import (
	"github.com/sjwhitworth/golearn/base"
)

type TrainFunctions interface {
	Fit(inst base.FixedDataGrid, learningRate float64) error
	Predict(X base.FixedDataGrid) (base.FixedDataGrid, error)
}

// TrainProcess fit and evaluate models for each parent peer.
func TrainProcess(instance *base.DenseInstances, to *TrainOptions, model TrainFunctions) error {
	err := model.Fit(instance, to.LearningRate)
	if err != nil {
		return err
	}
	return nil
}
