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
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ServerTestSuite struct {
	suite.Suite
	server *Server
	client client.ManagerClient
}

func (suite *ServerTestSuite) testMysqlConfig() *config.Config {
	return &config.Config{
		Server: &config.ServerConfig{
			Port: 8004,
		},
		ConfigService: &config.ConfigServiceConfig{
			StoreName: "store1",
		},
		Stores: []*config.StoreConfig{
			{
				Name: "store1",
				Type: "mysql",
				Mysql: &config.MysqlConfig{
					User:     "root",
					Password: "root1234",
					IP:       "127.0.0.1",
					Port:     3306,
					Db:       "config_db",
				},
				Oss: nil,
			},
		},
		HostService: &config.HostService{
			Skyline: &config.SkylineService{
				Domain:    "http://xxx",
				AppName:   "xxx",
				Account:   "xxx",
				AccessKey: "xxx",
			},
		},
	}
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

func (suite *ServerTestSuite) testDefaultCdnCluster() *types.CdnCluster {
	cdnConfigMap := map[string]string{
		"cdnConfig_a": "a",
		"cdnConfig_b": "b",
		"cdnConfig_c": "c",
	}

	cdnConfigBytes, _ := json.Marshal(&cdnConfigMap)
	return &types.CdnCluster{
		Config: string(cdnConfigBytes),
	}
}

//
//func (suite *ServerTestSuite) TestGetSchedulers() {
//	assert := assert.New(suite.T())
//
//	var cluster *types.SchedulerCluster
//	{
//		cluster = suite.testDefaultSchedulerCluster()
//		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
//		assert.NotNil(ret)
//		assert.Nil(err)
//		cluster.ClusterId = ret.ClusterId
//	}
//
//	hostName := "magneto-controller011162004111.nt12"
//	ip := "11.162.4.111"
//	var instance *types.SchedulerInstance
//	{
//		instance = &types.SchedulerInstance{
//			ClusterId:      cluster.ClusterId,
//			SecurityDomain: "security_abc",
//			Idc:            "idc_abc",
//			Location:       "location_abc",
//			NetConfig:      "",
//			HostName:       hostName,
//			Ip:             ip,
//			Port:           80,
//		}
//
//		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
//		assert.NotNil(ret)
//		assert.Nil(err)
//		instance.InstanceId = ret.InstanceId
//	}
//
//	{
//		req := &manager.GetSchedulersRequest{
//			Ip:       ip,
//			HostName: hostName,
//		}
//
//		ret, err := suite.server.ms.GetSchedulers(context.TODO(), req)
//		assert.NotNil(err)
//		assert.Nil(ret)
//	}
//
//	{
//		req := &manager.KeepAliveRequest{
//			HostName: hostName,
//			Type:     manager.ResourceType_Scheduler,
//		}
//
//		err := suite.server.ms.KeepAlive(context.TODO(), req)
//		assert.Nil(err)
//	}
//
//	{
//		req := &manager.GetSchedulersRequest{
//			Ip:       ip,
//			HostName: hostName,
//		}
//
//		ret, err := suite.server.ms.GetSchedulers(context.TODO(), req)
//		assert.Nil(err)
//		assert.NotNil(ret)
//	}
//}
//
//func (suite *ServerTestSuite) TestKeepAlive() {
//	assert := assert.New(suite.T())
//	configsvc.KeepAliveTimeoutMax = 5 * time.Second
//
//	{
//		req := &client.KeepAliveRequest{
//			IsCdn:       false,
//			IsScheduler: true,
//			Interval:    0,
//		}
//
//		err := suite.client.KeepAlive(context.TODO(), req)
//		assert.NotNil(err)
//	}
//
//	var cluster *types.SchedulerCluster
//	{
//		cluster = suite.testDefaultSchedulerCluster()
//		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
//		assert.NotNil(ret)
//		assert.Nil(err)
//		cluster.ClusterId = ret.ClusterId
//	}
//
//	var instance *types.SchedulerInstance
//	{
//		instance = &types.SchedulerInstance{
//			ClusterId:      cluster.ClusterId,
//			SecurityDomain: "security_abc",
//			Idc:            "idc_abc",
//			Location:       "location_abc",
//			NetConfig:      "",
//			HostName:       iputils.HostName,
//			Ip:             iputils.HostIp,
//			Port:           80,
//		}
//
//		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
//		assert.NotNil(ret)
//		assert.Nil(err)
//		instance.InstanceId = ret.InstanceId
//	}
//
//	{
//		req := &client.KeepAliveRequest{
//			IsCdn:       false,
//			IsScheduler: true,
//			Interval:    0,
//		}
//
//		err := suite.client.KeepAlive(context.TODO(), req)
//		assert.Nil(err)
//	}
//}
//
//func (suite *ServerTestSuite) TestGetClusterConfig() {
//	assert := assert.New(suite.T())
//
//	var cluster *types.SchedulerCluster
//	{
//		cluster = suite.testDefaultSchedulerCluster()
//		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
//		assert.NotNil(ret)
//		assert.Nil(err)
//		cluster.ClusterId = ret.ClusterId
//	}
//
//	var instance *types.SchedulerInstance
//	{
//		instance = &types.SchedulerInstance{
//			ClusterId:      cluster.ClusterId,
//			SecurityDomain: "security_abc",
//			Idc:            "idc_abc",
//			Location:       "location_abc",
//			NetConfig:      "",
//			HostName:       "dragonfly2-scheduler011239070235.nt12",
//			Ip:             "11.239.70.235",
//			Port:           80,
//		}
//
//		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
//		assert.NotNil(ret)
//		assert.Nil(err)
//		instance.InstanceId = ret.InstanceId
//	}
//
//	{
//		req := &manager.GetClusterConfigRequest{
//			HostName: "dragonfly2-scheduler011239070235.nt12",
//			Type:     manager.ResourceType_Scheduler,
//		}
//
//		ret, err := suite.server.ms.GetClusterConfig(context.TODO(), req)
//		assert.NotNil(err)
//		assert.Nil(ret)
//	}
//}

func (suite *ServerTestSuite) TestSchedulerCluster() {
	assert := assert.New(suite.T())

	var cluster *types.SchedulerCluster
	{
		cluster = suite.testDefaultSchedulerCluster()
		ret, err := suite.server.ms.AddSchedulerCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterId = ret.ClusterId
	}

	{
		ret, err := suite.server.ms.GetSchedulerCluster(context.TODO(), cluster.ClusterId)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(cluster.ClusterId, ret.ClusterId)
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
		assert.Equal(cluster.ClusterId, ret.ClusterId)
		assert.Equal(cluster.SchedulerConfig, ret.SchedulerConfig)
		assert.Equal(cluster.ClientConfig, ret.ClientConfig)
	}

	{
		ret, err := suite.server.ms.ListSchedulerClusters(context.TODO(), configsvc.WithMarker(0, 10))
		assert.NotNil(ret)
		assert.Nil(err)
		for i, c := range ret {
			if cluster.ClusterId == c.ClusterId {
				assert.Equal(cluster.ClusterId, ret[i].ClusterId)
				assert.Equal(cluster.SchedulerConfig, ret[i].SchedulerConfig)
				assert.Equal(cluster.ClientConfig, ret[i].ClientConfig)
			}
		}
	}

	{
		ret, err := suite.server.ms.DeleteSchedulerCluster(context.TODO(), cluster.ClusterId)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetSchedulerCluster(context.TODO(), cluster.ClusterId)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteSchedulerCluster(context.TODO(), fmt.Sprintf("%sabc", cluster.ClusterId))
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
		cluster.ClusterId = ret.ClusterId
	}

	var instance *types.SchedulerInstance
	{
		instance = &types.SchedulerInstance{
			ClusterId:      cluster.ClusterId,
			SecurityDomain: "security_abc",
			Idc:            "idc_abc",
			Location:       "location_abc",
			NetConfig:      "",
			HostName:       "hostname_abc",
			Ip:             "192.168.0.11",
			Port:           80,
		}

		ret, err := suite.server.ms.AddSchedulerInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceId = ret.InstanceId
	}

	{
		ret, err := suite.server.ms.GetSchedulerInstance(context.TODO(), instance.InstanceId)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceId, ret.InstanceId)
		assert.Equal(instance.SecurityDomain, ret.SecurityDomain)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		instance.SecurityDomain = "security_abc_update"
		ret, err := suite.server.ms.UpdateSchedulerInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceId, ret.InstanceId)
		assert.Equal(instance.SecurityDomain, ret.SecurityDomain)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		op := []configsvc.OpOption{}
		op = append(op, configsvc.WithClusterId(cluster.ClusterId))
		op = append(op, configsvc.WithMarker(0, 10))

		ret, err := suite.server.ms.ListSchedulerInstances(context.TODO(), op...)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceId, ret[0].InstanceId)
		assert.Equal(instance.SecurityDomain, ret[0].SecurityDomain)
		assert.Equal(instance.Location, ret[0].Location)
	}

	{
		ret, err := suite.server.ms.DeleteSchedulerInstance(context.TODO(), instance.InstanceId)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetSchedulerInstance(context.TODO(), instance.InstanceId)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteSchedulerInstance(context.TODO(), fmt.Sprintf("%sabc", instance.InstanceId))
		assert.Nil(ret)
		assert.Nil(err)
	}
}

func (suite *ServerTestSuite) TestCdnCluster() {
	assert := assert.New(suite.T())

	var cluster *types.CdnCluster
	{
		cluster = suite.testDefaultCdnCluster()
		ret, err := suite.server.ms.AddCdnCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterId = ret.ClusterId
	}

	{
		ret, err := suite.server.ms.GetCdnCluster(context.TODO(), cluster.ClusterId)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(cluster.ClusterId, ret.ClusterId)
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

		ret, err := suite.server.ms.UpdateCdnCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(cluster.ClusterId, ret.ClusterId)
		assert.Equal(cluster.Config, ret.Config)
	}

	{
		ret, err := suite.server.ms.ListCdnClusters(context.TODO(), configsvc.WithMarker(0, 10))
		assert.NotNil(ret)
		assert.Nil(err)
		for i, c := range ret {
			if cluster.ClusterId == c.ClusterId {
				assert.Equal(cluster.ClusterId, ret[i].ClusterId)
				assert.Equal(cluster.Config, ret[i].Config)
			}
		}
	}

	{
		ret, err := suite.server.ms.DeleteCdnCluster(context.TODO(), cluster.ClusterId)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetCdnCluster(context.TODO(), cluster.ClusterId)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteCdnCluster(context.TODO(), fmt.Sprintf("%sabc", cluster.ClusterId))
		assert.Nil(ret)
		assert.Nil(err)
	}
}

func (suite *ServerTestSuite) TestCdnInstance() {
	assert := assert.New(suite.T())

	var cluster *types.CdnCluster
	{
		cluster = suite.testDefaultCdnCluster()
		ret, err := suite.server.ms.AddCdnCluster(context.TODO(), cluster)
		assert.NotNil(ret)
		assert.Nil(err)
		cluster.ClusterId = ret.ClusterId
	}

	var instance *types.CdnInstance
	{
		instance = &types.CdnInstance{
			ClusterId: cluster.ClusterId,
			Idc:       "idc_abc",
			Location:  "location_abc",
			HostName:  "hostName_abc",
			Ip:        "ip_abc",
			Port:      0,
			RpcPort:   0,
			DownPort:  0,
		}

		ret, err := suite.server.ms.AddCdnInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		instance.InstanceId = ret.InstanceId
	}

	{
		ret, err := suite.server.ms.GetCdnInstance(context.TODO(), instance.InstanceId)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceId, ret.InstanceId)
		assert.Equal(instance.Idc, ret.Idc)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		instance.Location = "location_abc_update"
		ret, err := suite.server.ms.UpdateCdnInstance(context.TODO(), instance)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceId, ret.InstanceId)
		assert.Equal(instance.Idc, ret.Idc)
		assert.Equal(instance.Location, ret.Location)
	}

	{
		op := []configsvc.OpOption{}
		op = append(op, configsvc.WithClusterId(cluster.ClusterId))
		op = append(op, configsvc.WithMarker(0, 10))

		ret, err := suite.server.ms.ListCdnInstances(context.TODO(), op...)
		assert.NotNil(ret)
		assert.Nil(err)
		assert.Equal(instance.InstanceId, ret[0].InstanceId)
		assert.Equal(instance.Idc, ret[0].Idc)
		assert.Equal(instance.Location, ret[0].Location)
	}

	{
		ret, err := suite.server.ms.DeleteCdnInstance(context.TODO(), instance.InstanceId)
		assert.NotNil(ret)
		assert.Nil(err)

		ret, err = suite.server.ms.GetCdnInstance(context.TODO(), instance.InstanceId)
		assert.Nil(ret)
		assert.NotNil(err)

		ret, err = suite.server.ms.DeleteCdnInstance(context.TODO(), fmt.Sprintf("%sabc", instance.InstanceId))
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
		ret, err := suite.server.ms.ListSecurityDomains(context.TODO(), configsvc.WithMarker(0, 10))
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

func (suite *ServerTestSuite) SetupSuite() {
	assert := assert.New(suite.T())

	configsvc.KeepAliveTimeoutMax = 5 * time.Second
	_ = logcore.InitManager(false)
	cfg := suite.testMysqlConfig()
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
