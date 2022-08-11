package training

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
	LimitSendRate = 1
)

const (
	DataInstance  = "data_instance"
	LoadType      = "load_type"
	LoadData      = "load_data"
	LoadTest      = "load_test"
	OutPutModel   = "output_model"
	ManagerClient = "manager_client"
	DynConfigData = "dyn_config_data"
)
