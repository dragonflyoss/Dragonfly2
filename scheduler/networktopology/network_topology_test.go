package networktopology

import (
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client/mocks"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestNetworkTopology_NewNetworkTopology(t *testing.T) {
	tests := []struct {
		name   string
		config *config.Config
		expect func(t *testing.T, networkTopology NetworkTopology, err error)
	}{
		{
			name: "new network topology",
			config: &config.Config{
				Server: config.ServerConfig{
					Host:        "localhost",
					AdvertiseIP: net.ParseIP("127.0.0.1"),
					Port:        8080,
				},
				Host: config.HostConfig{
					IDC:      "foo",
					Location: "bar",
				},
				Manager: config.ManagerConfig{
					SchedulerClusterID: 1,
				},
			},
			expect: func(t *testing.T, n NetworkTopology, err error) {
				assert := assert.New(t)
				instance := n.(*networkTopology)
				assert.NotNil(instance.config)
				assert.NotNil(instance.resource)
				assert.NotNil(instance.managerClient)
				assert.NoError(err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctl := gomock.NewController(t)
			defer ctl.Finish()
			mockManagerClient := mocks.NewMockV2(ctl)
			res := resource.NewMockResource(ctl)
			n, err := NewNetworkTopology(tc.config, res, mockManagerClient, WithTransportCredentials(nil))
			tc.expect(t, n, err)
		})
	}

}
func TestNetworkTopology_GetHost(t *testing.T) {

}
