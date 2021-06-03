package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/manager/apis/v2/types"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/configsvc"
	"d7y.io/dragonfly/v2/manager/store"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct {
	suite.Suite
	server *Server
	client client.ManagerClient
}

func (suite *ServerTestSuite) testDefaultSchedulerCluster() *types.SchedulerCluster {
	schedulerConfigMap := map[string]string{
		"schedulerConfig_a": "a",
		"schedulerConfig_b": "b",
		"schedulerConfig_c": "c",
	}

	clientConfigMap := map[string]string{
		"clientConfig_a": "a",
		"clientConfig_b": "b",
		"clientConfig_c": "c",
	}

	schedulerConfigBytes, _ := json.Marshal(&schedulerConfigMap)
	clientConfigBytes, _ := json.Marshal(&clientConfigMap)

	return &types.SchedulerCluster{
		SchedulerConfig: string(schedulerConfigBytes),
		ClientConfig:    string(clientConfigBytes),
	}
}

func (suite *ServerTestSuite) testDefaultCDNCluster() *types.CDNCluster {
	cdnConfigMap := map[string]string{
		"cdnConfig_a": "a",
		"cdnConfig_b": "b",
		"cdnConfig_c": "c",
	}

	cdnConfigBytes, _ := json.Marshal(&cdnConfigMap)
	return &types.CDNCluster{
		Config: string(cdnConfigBytes),
	}
}

func (suite *ServerTestSuite) TestGetSchedulers() {
	assert := assert.New(suite.T())

	var cluster *types.SchedulerCluster
	{
		cluster = suite.testDefaultSchedulerCluster()
		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	var instance *types.SchedulerInstance
	{
		instance = &types.SchedulerInstance{
			ClusterID:      cluster.ClusterID,
			SecurityDomain: "security_abc",
			IDC:            "idc_abc",
			Location:       "location_abc",
			NetConfig:      "",
			HostName:       suite.randPrefix() + "hostname_abc",
			IP:             "192.168.0.11",
			Port:           80,
		}

		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceID = ret.InstanceID
	}

	{
		req := &manager.KeepAliveRequest{
			HostName: instance.HostName,
			Type:     manager.ResourceType_Scheduler,
		}

		err := suite.server.ms.KeepAlive(context.TODO(), req)
		assert.Nil(err)
	}

	{
		req := &manager.GetSchedulersRequest{
			HostName: instance.HostName,
		}

		ret, err := suite.server.ms.GetSchedulers(context.TODO(), req)
		assert.Nil(err)
		assert.NotNil(ret)
	}
}

func (suite *ServerTestSuite) TestKeepAliveScheduler() {
	assert := assert.New(suite.T())

	var cluster *types.SchedulerCluster
	{
		cluster = suite.testDefaultSchedulerCluster()
		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	var instance *types.SchedulerInstance
	{
		instance = &types.SchedulerInstance{
			ClusterID:      cluster.ClusterID,
			SecurityDomain: "security_abc",
			IDC:            "idc_abc",
			Location:       "location_abc",
			NetConfig:      "",
			HostName:       suite.randPrefix() + "hostname_abc",
			IP:             "192.168.0.11",
			Port:           80,
		}

		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceID = ret.InstanceID
	}

	for i := 0; i < 10; i++ {
		req := &manager.KeepAliveRequest{
			HostName: instance.HostName,
			Type:     manager.ResourceType_Scheduler,
		}

		err := suite.server.ms.KeepAlive(context.TODO(), req)
		assert.Nil(err)

		if i%2 == 0 {
			time.Sleep(configsvc.KeepAliveTimeoutMax - time.Second)

			ret, err := suite.server.ms.GetSchedulerInstance(context.TODO(), instance.InstanceID)
			assert.NotNil(ret)
			assert.Nil(err)
			assert.Equal(configsvc.InstanceActive, ret.State)
		} else {
			time.Sleep(configsvc.KeepAliveTimeoutMax * 2)

			ret, err := suite.server.ms.GetSchedulerInstance(context.TODO(), instance.InstanceID)
			assert.NotNil(ret)
			assert.Nil(err)
			assert.Equal(configsvc.InstanceInactive, ret.State)
		}
	}
}

func (suite *ServerTestSuite) TestKeepAliveCDN() {
	assert := assert.New(suite.T())

	var cluster *types.CDNCluster
	{
		cluster = suite.testDefaultCDNCluster()
		ret, err := suite.server.ms.AddCDNCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	var instance *types.CDNInstance
	{
		instance = &types.CDNInstance{
			ClusterID: cluster.ClusterID,
			IDC:       "idc_abc",
			Location:  "location_abc",
			HostName:  suite.randPrefix() + "hostname_abc",
			IP:        "ip_abc",
			Port:      0,
			RPCPort:   0,
			DownPort:  0,
		}

		ret, err := suite.server.ms.AddCDNInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceID = ret.InstanceID
	}

	for i := 0; i < 10; i++ {
		req := &manager.KeepAliveRequest{
			HostName: instance.HostName,
			Type:     manager.ResourceType_Cdn,
		}

		err := suite.server.ms.KeepAlive(context.TODO(), req)
		assert.Nil(err)

		if i%2 == 0 {
			time.Sleep(configsvc.KeepAliveTimeoutMax - time.Second)

			ret, err := suite.server.ms.GetCDNInstance(context.TODO(), instance.InstanceID)
			assert.NotNil(ret)
			assert.Nil(err)
			assert.Equal(configsvc.InstanceActive, ret.State)
		} else {
			time.Sleep(configsvc.KeepAliveTimeoutMax * 2)

			ret, err := suite.server.ms.GetCDNInstance(context.TODO(), instance.InstanceID)
			assert.NotNil(ret)
			assert.Nil(err)
			assert.Equal(configsvc.InstanceInactive, ret.State)
		}
	}
}

func (suite *ServerTestSuite) TestGetSchedulerClusterConfig() {
	assert := assert.New(suite.T())

	var cluster *types.SchedulerCluster
	{
		cluster = suite.testDefaultSchedulerCluster()
		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	var instance *types.SchedulerInstance
	{
		instance = &types.SchedulerInstance{
			ClusterID:      cluster.ClusterID,
			SecurityDomain: "security_abc",
			IDC:            "idc_abc",
			Location:       "location_abc",
			NetConfig:      "",
			HostName:       suite.randPrefix() + "hostname_abc",
			IP:             "192.168.0.11",
			Port:           80,
		}

		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceID = ret.InstanceID
	}

	{
		req := &manager.GetClusterConfigRequest{
			HostName: instance.HostName,
			Type:     manager.ResourceType_Scheduler,
		}

		ret, err := suite.server.ms.GetClusterConfig(context.TODO(), req)
		assert.Nil(err)
		assert.NotNil(ret)
		cfg := ret.GetSchedulerConfig()
		assert.Equal(cluster.SchedulerConfig, cfg.ClusterConfig)
		assert.Equal(cluster.ClientConfig, cfg.ClientConfig)
	}

	var cdnCluster *types.CDNCluster
	{
		cdnCluster = suite.testDefaultCDNCluster()
		ret, err := suite.server.ms.AddCDNCluster(context.TODO(), cdnCluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cdnCluster.ClusterID = ret.ClusterID
	}

	var cdnInstance *types.CDNInstance
	{
		cdnInstance = &types.CDNInstance{
			ClusterID: cdnCluster.ClusterID,
			IDC:       "idc_abc",
			Location:  "location_abc",
			HostName:  suite.randPrefix() + "hostname_abc",
			IP:        "ip_abc",
			Port:      0,
			RPCPort:   0,
			DownPort:  0,
		}

		ret, err := suite.server.ms.AddCDNInstance(context.TODO(), cdnInstance)
		assert.NotNil(ret)
		assert.Nil(err)
		cdnInstance.InstanceID = ret.InstanceID
	}

	{
		req := &manager.KeepAliveRequest{
			HostName: cdnInstance.HostName,
			Type:     manager.ResourceType_Cdn,
		}

		err := suite.server.ms.KeepAlive(context.TODO(), req)
		assert.Nil(err)
	}

	{
		var schedulerConfigMap map[string]string
		err := json.Unmarshal([]byte(cluster.SchedulerConfig), &schedulerConfigMap)
		assert.Nil(err)

		schedulerConfigMap["CDN_CLUSTER_ID"] = cdnCluster.ClusterID
		schedulerConfigByte, err := json.Marshal(schedulerConfigMap)
		assert.Nil(err)
		cluster.SchedulerConfig = string(schedulerConfigByte)

		suite.server.ms.UpdateSchedulerCluster(context.TODO(), cluster)
	}

	{
		req := &manager.GetClusterConfigRequest{
			HostName: instance.HostName,
			Type:     manager.ResourceType_Scheduler,
		}

		ret, err := suite.server.ms.GetClusterConfig(context.TODO(), req)
		assert.Nil(err)
		assert.NotNil(ret)
		cfg := ret.GetSchedulerConfig()
		assert.Equal(cluster.SchedulerConfig, cfg.ClusterConfig)
		assert.Equal(cluster.ClientConfig, cfg.ClientConfig)

		cdnHost := cfg.GetCdnHosts()
		assert.Equal(1, len(cdnHost))

		assert.Equal(cdnInstance.HostName, cdnHost[0].HostInfo.HostName)
		assert.Equal(cdnInstance.IP, cdnHost[0].HostInfo.Ip)
	}
}

func (suite *ServerTestSuite) TestGetCDNClusterConfig() {
	assert := assert.New(suite.T())

	var cluster *types.CDNCluster
	{
		cluster = suite.testDefaultCDNCluster()
		ret, err := suite.server.ms.AddCDNCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	var instance *types.CDNInstance
	{
		instance = &types.CDNInstance{
			ClusterID: cluster.ClusterID,
			IDC:       "idc_abc",
			Location:  "location_abc",
			HostName:  suite.randPrefix() + "hostname_abc",
			IP:        "ip_abc",
			Port:      0,
			RPCPort:   0,
			DownPort:  0,
		}

		ret, err := suite.server.ms.AddCDNInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceID = ret.InstanceID
	}

	{
		req := &manager.GetClusterConfigRequest{
			HostName: instance.HostName,
			Type:     manager.ResourceType_Cdn,
		}

		ret, err := suite.server.ms.GetClusterConfig(context.TODO(), req)
		assert.Nil(err)
		assert.NotNil(ret)
		cfg := ret.GetCdnConfig()
		assert.Equal(cluster.Config, cfg.ClusterConfig)
	}
}

func (suite *ServerTestSuite) TestSchedulerCluster() {
	assert := assert.New(suite.T())

	var cluster *types.SchedulerCluster
	{
		cluster = suite.testDefaultSchedulerCluster()
		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	{
		ret, err := suite.server.ms.GetSchedulerCluster(context.TODO(), cluster.ClusterID)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(cluster.ClusterID, ret.ClusterID)
		assert.Equal(cluster.SchedulerConfig, ret.SchedulerConfig)
		assert.Equal(cluster.ClientConfig, ret.ClientConfig)
	}

	{
		var schedulerConfigMap map[string]string
		err := json.Unmarshal([]byte(cluster.SchedulerConfig), &schedulerConfigMap)
		assert.Nil(err)

		schedulerConfigMap["schedulerConfig_a"] = "schedulerConfig_a_update"
		schedulerConfigByte, err := json.Marshal(schedulerConfigMap)
		assert.Nil(err)
		cluster.SchedulerConfig = string(schedulerConfigByte)

		ret, err := suite.server.ms.UpdateSchedulerCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(cluster.ClusterID, ret.ClusterID)
		assert.Equal(cluster.SchedulerConfig, ret.SchedulerConfig)
		assert.Equal(cluster.ClientConfig, ret.ClientConfig)
	}

	{
		ret, err := suite.server.ms.ListSchedulerClusters(context.TODO(), store.WithMarker(0, 10))
		assert.NotNil(ret)
		assert.Nil(err)
		for i, c := range ret {
			if cluster.ClusterID == c.ClusterID {
				assert.Equal(cluster.ClusterID, ret[i].ClusterID)
				assert.Equal(cluster.SchedulerConfig, ret[i].SchedulerConfig)
				assert.Equal(cluster.ClientConfig, ret[i].ClientConfig)
			}
		}
	}

	{
		ret, err := suite.server.ms.DeleteSchedulerCluster(context.TODO(), cluster.ClusterID)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetSchedulerCluster(context.TODO(), cluster.ClusterID)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteSchedulerCluster(context.TODO(), fmt.Sprintf("%sabc", cluster.ClusterID))
		assert.Nil(ret)
		assert.Nil(err)
	}
}

func (suite *ServerTestSuite) TestSchedulerInstance() {
	assert := assert.New(suite.T())

	var cluster *types.SchedulerCluster
	{
		cluster = suite.testDefaultSchedulerCluster()
		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	var instance *types.SchedulerInstance
	{
		instance = &types.SchedulerInstance{
			ClusterID:      cluster.ClusterID,
			SecurityDomain: "security_abc",
			IDC:            "idc_abc",
			Location:       "location_abc",
			NetConfig:      "",
			HostName:       suite.randPrefix() + "hostname_abc",
			IP:             "192.168.0.11",
			Port:           80,
		}

		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceID = ret.InstanceID
	}

	{
		ret, err := suite.server.ms.GetSchedulerInstance(context.TODO(), instance.InstanceID)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceID, ret.InstanceID)
		assert.Equal(instance.SecurityDomain, ret.SecurityDomain)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		instance.SecurityDomain = "security_abc_update"
		ret, err := suite.server.ms.UpdateSchedulerInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceID, ret.InstanceID)
		assert.Equal(instance.SecurityDomain, ret.SecurityDomain)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		op := []store.OpOption{}
		op = append(op, store.WithClusterID(cluster.ClusterID))
		op = append(op, store.WithMarker(0, 10))

		ret, err := suite.server.ms.ListSchedulerInstances(context.TODO(), op...)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceID, ret[0].InstanceID)
		assert.Equal(instance.SecurityDomain, ret[0].SecurityDomain)
		assert.Equal(instance.Location, ret[0].Location)
	}

	{
		ret, err := suite.server.ms.DeleteSchedulerInstance(context.TODO(), instance.InstanceID)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetSchedulerInstance(context.TODO(), instance.InstanceID)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteSchedulerInstance(context.TODO(), fmt.Sprintf("%sabc", instance.InstanceID))
		assert.Nil(ret)
		assert.Nil(err)
	}
}

func (suite *ServerTestSuite) TestCDNCluster() {
	assert := assert.New(suite.T())

	var cluster *types.CDNCluster
	{
		cluster = suite.testDefaultCDNCluster()
		ret, err := suite.server.ms.AddCDNCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	{
		ret, err := suite.server.ms.GetCDNCluster(context.TODO(), cluster.ClusterID)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(cluster.ClusterID, ret.ClusterID)
		assert.Equal(cluster.Config, ret.Config)
	}

	{
		var cdnConfigMap map[string]string
		err := json.Unmarshal([]byte(cluster.Config), &cdnConfigMap)
		assert.Nil(err)

		cdnConfigMap["cdnConfig_a"] = "cdnConfig_a_update"
		cdnConfigByte, err := json.Marshal(cdnConfigMap)
		assert.Nil(err)
		cluster.Config = string(cdnConfigByte)

		ret, err := suite.server.ms.UpdateCDNCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(cluster.ClusterID, ret.ClusterID)
		assert.Equal(cluster.Config, ret.Config)
	}

	{
		ret, err := suite.server.ms.ListCDNClusters(context.TODO(), store.WithMarker(0, 10))
		assert.NotNil(ret)
		assert.Nil(err)
		for i, c := range ret {
			if cluster.ClusterID == c.ClusterID {
				assert.Equal(cluster.ClusterID, ret[i].ClusterID)
				assert.Equal(cluster.Config, ret[i].Config)
			}
		}
	}

	{
		ret, err := suite.server.ms.DeleteCDNCluster(context.TODO(), cluster.ClusterID)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetCDNCluster(context.TODO(), cluster.ClusterID)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteCDNCluster(context.TODO(), fmt.Sprintf("%sabc", cluster.ClusterID))
		assert.Nil(ret)
		assert.Nil(err)
	}
}

func (suite *ServerTestSuite) TestCDNInstance() {
	assert := assert.New(suite.T())

	var cluster *types.CDNCluster
	{
		cluster = suite.testDefaultCDNCluster()
		ret, err := suite.server.ms.AddCDNCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterID = ret.ClusterID
	}

	var instance *types.CDNInstance
	{
		instance = &types.CDNInstance{
			ClusterID: cluster.ClusterID,
			IDC:       "idc_abc",
			Location:  "location_abc",
			HostName:  suite.randPrefix() + "hostname_abc",
			IP:        "ip_abc",
			Port:      0,
			RPCPort:   0,
			DownPort:  0,
		}

		ret, err := suite.server.ms.AddCDNInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceID = ret.InstanceID
	}

	{
		ret, err := suite.server.ms.GetCDNInstance(context.TODO(), instance.InstanceID)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceID, ret.InstanceID)
		assert.Equal(instance.IDC, ret.IDC)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		instance.Location = "location_abc_update"
		ret, err := suite.server.ms.UpdateCDNInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceID, ret.InstanceID)
		assert.Equal(instance.IDC, ret.IDC)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		op := []store.OpOption{}
		op = append(op, store.WithClusterID(cluster.ClusterID))
		op = append(op, store.WithMarker(0, 10))

		ret, err := suite.server.ms.ListCDNInstances(context.TODO(), op...)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceID, ret[0].InstanceID)
		assert.Equal(instance.IDC, ret[0].IDC)
		assert.Equal(instance.Location, ret[0].Location)
	}

	{
		ret, err := suite.server.ms.DeleteCDNInstance(context.TODO(), instance.InstanceID)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetCDNInstance(context.TODO(), instance.InstanceID)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteCDNInstance(context.TODO(), fmt.Sprintf("%sabc", instance.InstanceID))
		assert.Nil(ret)
		assert.Nil(err)
	}
}

func (suite *ServerTestSuite) testDefaultSecurityDomain() *types.SecurityDomain {
	var proxyDomainMap = map[string]string{
		"proxyDomain_a": "proxyDomain_a",
		"proxyDomain_b": "proxyDomain_b",
		"proxyDomain_c": "proxyDomain_c",
	}

	proxyDomainByte, _ := json.Marshal(proxyDomainMap)
	domain := fmt.Sprintf("securityDomain_%d", time.Now().Unix())
	return &types.SecurityDomain{
		SecurityDomain: domain,
		DisplayName:    domain,
		ProxyDomain:    string(proxyDomainByte),
	}
}

func (suite *ServerTestSuite) TestSecurityDomain() {
	assert := assert.New(suite.T())

	var domain *types.SecurityDomain
	{
		domain = suite.testDefaultSecurityDomain()
		ret, err := suite.server.ms.AddSecurityDomain(context.TODO(), domain)
		assert.NotNil(ret)
		assert.Nil(err)
	}

	{
		ret, err := suite.server.ms.GetSecurityDomain(context.TODO(), domain.SecurityDomain)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(domain.SecurityDomain, ret.SecurityDomain)
		assert.Equal(domain.ProxyDomain, ret.ProxyDomain)
	}

	{
		var proxyDomainMap map[string]string
		err := json.Unmarshal([]byte(domain.ProxyDomain), &proxyDomainMap)
		assert.Nil(err)

		proxyDomainMap["proxyDomain_a"] = "proxyDomain_a_update"
		proxyDomainByte, err := json.Marshal(proxyDomainMap)
		assert.Nil(err)
		domain.ProxyDomain = string(proxyDomainByte)

		ret, err := suite.server.ms.UpdateSecurityDomain(context.TODO(), domain)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(domain.SecurityDomain, ret.SecurityDomain)
		assert.Equal(domain.ProxyDomain, ret.ProxyDomain)
	}

	{
		ret, err := suite.server.ms.ListSecurityDomains(context.TODO(), store.WithMarker(0, 10))
		assert.NotNil(ret)
		assert.Nil(err)
		for i, d := range ret {
			if domain.SecurityDomain == d.SecurityDomain {
				assert.Equal(domain.ProxyDomain, ret[i].ProxyDomain)
			}
		}
	}

	{
		ret, err := suite.server.ms.DeleteSecurityDomain(context.TODO(), domain.SecurityDomain)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetSecurityDomain(context.TODO(), domain.SecurityDomain)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteSecurityDomain(context.TODO(), domain.SecurityDomain)
		assert.Nil(ret)
		assert.Nil(err)
	}
}

func (suite *ServerTestSuite) randPrefix() string {
	return fmt.Sprintf("%d_", time.Now().Unix())
}

func (suite *ServerTestSuite) SetupSuite() {
	assert := assert.New(suite.T())

	configsvc.KeepAliveTimeoutMax = 2 * time.Second
	_ = logcore.InitManager(false)
	cfg := config.New()
	server, err := New(cfg)
	assert.Nil(err)
	assert.NotNil(server)
	suite.server = server

	go server.Serve()

	addr := fmt.Sprintf("%s:%d", cfg.Server.IP, cfg.Server.Port)
	client, err := client.NewClient([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: addr,
		},
	})
	assert.Nil(err)
	assert.NotNil(client)
	suite.client = client
}

func (suite *ServerTestSuite) TearDownSuite() {
	suite.server.Stop()
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
