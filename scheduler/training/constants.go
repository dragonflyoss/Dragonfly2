package training

// TODO move to config?
const (
	// NormalizedFieldNum field which needs normalization lies at the end of record.
	NormalizedFieldNum = 7

	// DefaultMaxBufferLine capacity of lines which read from record file.
	DefaultMaxBufferLine = 64

	// DefaultMaxRecordLine capacity of lines which local memory obtains.
	DefaultMaxRecordLine = 100000

	// TestSetPercent percent of test set.
	TestSetPercent = 0.2

	// DefaultLearningRate default learning rate of model.
	DefaultLearningRate = 0.01

	// InitSum initiate sum of accuracy.
	InitSum = 0.0
)

const (
	// DefaultDir default dir of record file.
	DefaultDir = "/tmp"

	// DefaultRecord default file name of record file.
	DefaultRecord = "record.csv"
)

const (
	Instance = "instance"
	BaseDir  = "baseDir"
	FileName = "fileName"
)
