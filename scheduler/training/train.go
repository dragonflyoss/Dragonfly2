package training

import (
	"github.com/sjwhitworth/golearn/base"
)

type TrainFunctions interface {
	Fit(inst base.FixedDataGrid, learningRate float64) error
	Predict(X base.FixedDataGrid) (base.FixedDataGrid, error)
}

type TrainOptions struct {
	LearningRate float64
}

type TrainOptionFunc func(options *TrainOptions)

func WithLearningRate(LearningRate float64) TrainOptionFunc {
	return func(options *TrainOptions) {
		options.LearningRate = LearningRate
	}
}

func NewTrainOptions() *TrainOptions {
	return &TrainOptions{
		LearningRate: DefaultLearningRate,
	}
}

// TrainProcess fit and evaluate models for each parent peer.
func TrainProcess(instance *base.DenseInstances, to *TrainOptions, model TrainFunctions) error {
	err := model.Fit(instance, to.LearningRate)
	if err != nil {
		return err
	}
	return nil
}
