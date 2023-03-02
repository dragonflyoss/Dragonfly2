package trainer

const (
	// DefaultGRPCPort is default port for grpc server.
	DefaultGRPCPort = 65003

	// DefaultTrainerKeepAliveInterval is default interval for keepalive.
	DefaultTrainerKeepAliveInterval = 50000
)

const (
	// DefaultMetricsAddr is default address for metrics server.
	DefaultMetricsAddr = ":8000"
)

const (
	// DefaultTrainerDB is default db for redis trainer
	DefaultTrainerDB = 3
)

var (
	// DefaultMaxBackups is default saving model number
	DefaultMaxBackups = 5
	// DefaultSchemaPath is default path for storing training data
	DefaultSchemaPath = "/trainer/data"
)

var (
	// DefaultNetworkEnableIPv6 is default value of enableIPv6.
	DefaultNetworkEnableIPv6 = false
)
