package trainer

import (
	"net"
	"os"
	"testing"

	testifyassert "github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestTrainerConfig_Load(t *testing.T) {
	assert := testifyassert.New(t)

	config := &Config{
		Server: ServerConfig{
			GRPC: GRPCConfig{
				AdvertiseIP: net.IPv4zero,
				ListenIP:    net.IPv4zero,
				PortRange: TCPListenPortRange{
					Start: 65003,
					End:   65003,
				},
			},
			KeepAlive: KeepAliveConfig{
				Interval: 5000000000,
			},
		},
		Metrics: MetricsConfig{
			Enable: true,
			Addr:   ":8000",
		},
		Redis: RedisConfig{
			Addrs:      []string{"foo", "bar"},
			MasterName: "baz",
			Password:   "bar",
			TrainerDB:  3,
		},
		Trainer: TrainerConfig{
			MaxBackups: 5,
			DataPath:   "/trainer/data",
		},
		Network: NetworkConfig{
			EnableIPv6: true,
		},
	}

	trainerConfigYAML := &Config{}
	contentYAML, _ := os.ReadFile("./testdata/trainer.yaml")
	if err := yaml.Unmarshal(contentYAML, &trainerConfigYAML); err != nil {
		t.Fatal(err)
	}

	assert.EqualValues(config, trainerConfigYAML)
}
