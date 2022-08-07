package training

type TrainOptions struct {
	// maxBufferLine capacity of lines which reads from record once.
	MaxBufferLine int

	// maxRecordLine capacity of lines which local memory obtains.
	MaxRecordLine int

	// learningRate learning rate of model.
	LearningRate float64
}

type TrainOptionFunc func(options *TrainOptions)

func WithMaxBufferLine(MaxBufferLine int) TrainOptionFunc {
	return func(options *TrainOptions) {
		options.MaxBufferLine = MaxBufferLine
	}
}

func WithMaxRecordLine(MaxRecordLine int) TrainOptionFunc {
	return func(options *TrainOptions) {
		options.MaxRecordLine = MaxRecordLine
	}
}

func WithLearningRate(LearningRate float64) TrainOptionFunc {
	return func(options *TrainOptions) {
		options.LearningRate = LearningRate
	}
}
