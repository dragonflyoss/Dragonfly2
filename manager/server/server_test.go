package server

import (
	"context"
	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dflog/logcore"
	"d7y.io/dragonfly/v2/pkg/rpc"
	"d7y.io/dragonfly/v2/pkg/rpc/manager"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type ServerTestSuite struct {
	suite.Suite
	server *Server
	client client.ManagerClient
}

func genTestSchedulerConfig(ip, hostName string) *manager.SchedulerConfig {
	return &manager.SchedulerConfig{
		ClientConfig: nil,
		CdnHosts: []*manager.ServerInfo{
			{
				HostInfo: &manager.HostInfo{
					Ip:             ip,
					HostName:       hostName,
					SecurityDomain: "securityDomain",
					Location:       "location",
					Idc:            "idc",
					NetTopology:    "netTopology",
				},
				RpcPort:  0,
				DownPort: 0,
			},
		},
	}
}

func genTestCdnConfig() *manager.CdnConfig {
	return &manager.CdnConfig{}
}

func (suite *ServerTestSuite) memoryConfig() *config.Config {
	return &config.Config{
		Server: &config.ServerConfig{
			Port: 8004,
		},
		Stores: []*config.StoreConfig{
			{
				Name:   "memoryStore",
				Type:   "memory",
				Memory: &config.MemoryConfig{},
				Oss:    nil,
				Mysql:  nil,
			},
		},
		ConfigService: &config.ConfigServiceConfig{
			StoreName: "memoryStore",
		},
	}
}

func (suite *ServerTestSuite) mysqlConfig() *config.Config {
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
					Username: "root",
					Password: "root1234",
					IP:       "127.0.0.1",
					Port:     3306,
					DbName:   "config_db",
				},
				Oss:    nil,
				Memory: nil,
			},
		},
	}
}

func (suite *ServerTestSuite) TestAddConfig() {
	assert := assert.New(suite.T())

	for i := 0; i < 1000; i++ {
		object := fmt.Sprintf("objcet%d", i)
		hostName := fmt.Sprintf("hostName%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Scheduler,
				Version: 0,
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: genTestSchedulerConfig("192.168.0.10", hostName)},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		getReq := &manager.GetConfigRequest{
			Id: addRep.GetId(),
		}

		getRep, err := suite.client.GetConfig(context.TODO(), getReq)
		assert.Nil(err)

		assert.Equal(addRep.GetId(), getReq.GetId())
		assert.Equal(object, getRep.Config.GetObject())
		assert.Equal(manager.ObjType_Scheduler, getRep.Config.GetObjType())
		assert.Equal(hostName, getRep.Config.GetSchedulerConfig().GetCdnHosts()[0].GetHostInfo().GetHostName())
	}

	for i := 1000; i < 2000; i++ {
		object := fmt.Sprintf("objcet%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Cdn,
				Version: 0,
				Body:    &manager.Config_CdnConfig{CdnConfig: genTestCdnConfig()},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		getReq := &manager.GetConfigRequest{
			Id: addRep.GetId(),
		}

		getRep, err := suite.client.GetConfig(context.TODO(), getReq)
		assert.Nil(err)

		assert.Equal(addRep.GetId(), getReq.GetId())
		assert.Equal(addRep.GetId(), getReq.GetId())
		assert.Equal(object, getRep.Config.GetObject())
		assert.Equal(manager.ObjType_Cdn, getRep.Config.GetObjType())
	}
}

func (suite *ServerTestSuite) TestDeleteConfig() {
	assert := assert.New(suite.T())

	for i := 0; i < 1000; i++ {
		object := fmt.Sprintf("objcet%d", i)
		hostName := fmt.Sprintf("hostName%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Scheduler,
				Version: 0,
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: genTestSchedulerConfig("192.168.0.10", hostName)},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		deleteReq := &manager.DeleteConfigRequest{
			Id: addRep.GetId(),
		}

		/* first delete */
		deleteRep, err := suite.client.DeleteConfig(context.TODO(), deleteReq)
		assert.Nil(err)
		assert.True(deleteRep.GetState().GetSuccess())

		/* second delete */
		deleteRep, err = suite.client.DeleteConfig(context.TODO(), deleteReq)
		assert.Nil(err)
		assert.True(deleteRep.GetState().GetSuccess())

		/* get */
		getReq := &manager.GetConfigRequest{
			Id: addRep.GetId(),
		}

		getRep, err := suite.client.GetConfig(context.TODO(), getReq)
		assert.NotNil(err)
		assert.Nil(getRep)
	}

	for i := 1000; i < 2000; i++ {
		object := fmt.Sprintf("objcet%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Cdn,
				Version: 0,
				Body:    &manager.Config_CdnConfig{CdnConfig: genTestCdnConfig()},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		deleteReq := &manager.DeleteConfigRequest{
			Id: addRep.GetId(),
		}

		/* first delete */
		deleteRep, err := suite.client.DeleteConfig(context.TODO(), deleteReq)
		assert.Nil(err)
		assert.True(deleteRep.GetState().GetSuccess())

		/* second delete */
		deleteRep, err = suite.client.DeleteConfig(context.TODO(), deleteReq)
		assert.Nil(err)
		assert.True(deleteRep.GetState().GetSuccess())

		/* get */
		getReq := &manager.GetConfigRequest{
			Id: addRep.GetId(),
		}

		getRep, err := suite.client.GetConfig(context.TODO(), getReq)
		assert.NotNil(err)
		assert.Nil(getRep)
	}
}

func (suite *ServerTestSuite) TestUpdateConfig() {
	assert := assert.New(suite.T())

	for i := 0; i < 1000; i++ {
		object := fmt.Sprintf("objcet%d", i)
		hostName := fmt.Sprintf("hostName%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Scheduler,
				Version: 0,
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: genTestSchedulerConfig("192.168.0.10", hostName)},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		/* first get */
		getReq := &manager.GetConfigRequest{
			Id: addRep.GetId(),
		}

		getRep, err := suite.client.GetConfig(context.TODO(), getReq)
		assert.Nil(err)

		assert.Equal(addRep.GetId(), getReq.GetId())
		assert.Equal(object, getRep.Config.GetObject())
		assert.Equal(manager.ObjType_Scheduler, getRep.Config.GetObjType())
		assert.Equal(hostName, getRep.Config.GetSchedulerConfig().GetCdnHosts()[0].GetHostInfo().GetHostName())

		/* update */
		hostName = fmt.Sprintf("hostName_%d", i)
		updateReq := &manager.UpdateConfigRequest{
			Id: addRep.GetId(),
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Scheduler,
				Version: 1,
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: genTestSchedulerConfig("192.168.0.10", hostName)},
			},
		}

		updateRep, err := suite.client.UpdateConfig(context.TODO(), updateReq)
		assert.Nil(err)
		assert.True(updateRep.GetState().GetSuccess())

		/* second get */
		getRep, err = suite.client.GetConfig(context.TODO(), getReq)
		assert.Nil(err)

		assert.Equal(addRep.GetId(), getReq.GetId())
		assert.Equal(object, getRep.Config.GetObject())
		assert.Equal(manager.ObjType_Scheduler, getRep.Config.GetObjType())
		assert.Equal(uint64(1), getRep.Config.GetVersion())
		assert.Equal(hostName, getRep.Config.GetSchedulerConfig().GetCdnHosts()[0].GetHostInfo().GetHostName())
	}

	for i := 1000; i < 2000; i++ {
		object := fmt.Sprintf("objcet%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Cdn,
				Version: 0,
				Body:    &manager.Config_CdnConfig{CdnConfig: genTestCdnConfig()},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		/* first get */
		getReq := &manager.GetConfigRequest{
			Id: addRep.GetId(),
		}

		getRep, err := suite.client.GetConfig(context.TODO(), getReq)
		assert.Nil(err)

		assert.Equal(addRep.GetId(), getReq.GetId())
		assert.Equal(object, getRep.Config.GetObject())
		assert.Equal(manager.ObjType_Cdn, getRep.Config.GetObjType())

		/* update */
		hostName := fmt.Sprintf("hostName%d", i)
		updateReq := &manager.UpdateConfigRequest{
			Id: addRep.GetId(),
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Scheduler,
				Version: 1,
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: genTestSchedulerConfig("192.168.0.10", hostName)},
			},
		}

		updateRep, err := suite.client.UpdateConfig(context.TODO(), updateReq)
		assert.Nil(err)
		assert.True(updateRep.GetState().GetSuccess())

		/* second get */
		getRep, err = suite.client.GetConfig(context.TODO(), getReq)
		assert.Nil(err)

		assert.Equal(addRep.GetId(), getReq.GetId())
		assert.Equal(object, getRep.Config.GetObject())
		assert.Equal(manager.ObjType_Scheduler, getRep.Config.GetObjType())
		assert.Equal(uint64(1), getRep.Config.GetVersion())
		assert.Equal(hostName, getRep.Config.GetSchedulerConfig().GetCdnHosts()[0].GetHostInfo().GetHostName())
	}
}

func (suite *ServerTestSuite) TestListConfig() {
	assert := assert.New(suite.T())

	object := fmt.Sprintf("object-%s", time.Now().String())

	for i := 0; i < 100; i++ {
		hostName := fmt.Sprintf("hostName%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Scheduler,
				Version: 0,
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: genTestSchedulerConfig("192.168.0.10", hostName)},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		listReq := &manager.ListConfigsRequest{
			Object: object,
		}

		listRep, err := suite.client.ListConfigs(context.TODO(), listReq)
		assert.Nil(err)
		assert.True(listRep.GetState().GetSuccess())
		assert.Equal(i+1, len(listRep.GetConfigs()))
	}

	for i := 100; i < 200; i++ {
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Cdn,
				Version: 0,
				Body:    &manager.Config_CdnConfig{CdnConfig: genTestCdnConfig()},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		listReq := &manager.ListConfigsRequest{
			Object: object,
		}

		listRep, err := suite.client.ListConfigs(context.TODO(), listReq)
		assert.Nil(err)
		assert.True(listRep.GetState().GetSuccess())
		assert.Equal(i+1, len(listRep.GetConfigs()))
	}

	{
		listReq := &manager.ListConfigsRequest{
			Object: object,
		}

		listRep, err := suite.client.ListConfigs(context.TODO(), listReq)
		assert.Nil(err)
		assert.True(listRep.GetState().GetSuccess())

		for _, config := range listRep.GetConfigs() {
			deleteReq := &manager.DeleteConfigRequest{
				Id: config.GetId(),
			}

			deleteRep, err := suite.client.DeleteConfig(context.TODO(), deleteReq)
			assert.Nil(err)
			assert.True(deleteRep.GetState().GetSuccess())
		}
	}
}

func (suite *ServerTestSuite) TestKeepAlive() {
	assert := assert.New(suite.T())

	object := fmt.Sprintf("object-%s", time.Now().String())

	for i := 0; i < 100; i++ {
		hostName := fmt.Sprintf("hostName%d", i)
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Scheduler,
				Version: uint64(i),
				Body:    &manager.Config_SchedulerConfig{SchedulerConfig: genTestSchedulerConfig("192.168.0.10", hostName)},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		listReq := &manager.ListConfigsRequest{
			Object: object,
		}

		listRep, err := suite.client.ListConfigs(context.TODO(), listReq)
		assert.Nil(err)
		assert.True(listRep.GetState().GetSuccess())
		assert.Equal(i+1, len(listRep.GetConfigs()))
	}

	{
		keepaliveReq := &manager.KeepAliveRequest{
			Object:  object,
			ObjType: manager.ObjType_Scheduler,
		}

		keepaliveRep, err := suite.client.KeepAlive(context.TODO(), keepaliveReq)
		assert.Nil(err)
		assert.Equal(object, keepaliveRep.GetConfig().GetObject())
		assert.Equal(manager.ObjType_Scheduler, keepaliveRep.GetConfig().GetObjType())
		assert.NotNil(99, keepaliveRep.GetConfig().GetVersion())
	}

	for i := 100; i < 200; i++ {
		addReq := &manager.AddConfigRequest{
			Config: &manager.Config{
				Object:  object,
				ObjType: manager.ObjType_Cdn,
				Version: uint64(i),
				Body:    &manager.Config_CdnConfig{CdnConfig: genTestCdnConfig()},
			},
		}

		addRep, err := suite.client.AddConfig(context.TODO(), addReq)
		assert.Nil(err)
		assert.True(addRep.GetState().Success)

		listReq := &manager.ListConfigsRequest{
			Object: object,
		}

		listRep, err := suite.client.ListConfigs(context.TODO(), listReq)
		assert.Nil(err)
		assert.True(listRep.GetState().GetSuccess())
		assert.Equal(i+1, len(listRep.GetConfigs()))
	}

	{
		keepaliveReq := &manager.KeepAliveRequest{
			Object:  object,
			ObjType: manager.ObjType_Cdn,
		}

		keepaliveRep, err := suite.client.KeepAlive(context.TODO(), keepaliveReq)
		assert.Nil(err)
		assert.Equal(object, keepaliveRep.GetConfig().GetObject())
		assert.Equal(manager.ObjType_Cdn, keepaliveRep.GetConfig().GetObjType())
		assert.NotNil(199, keepaliveRep.GetConfig().GetVersion())
	}

	{
		listReq := &manager.ListConfigsRequest{
			Object: object,
		}

		listRep, err := suite.client.ListConfigs(context.TODO(), listReq)
		assert.Nil(err)
		assert.True(listRep.GetState().GetSuccess())

		for _, config := range listRep.GetConfigs() {
			deleteReq := &manager.DeleteConfigRequest{
				Id: config.GetId(),
			}

			deleteRep, err := suite.client.DeleteConfig(context.TODO(), deleteReq)
			assert.Nil(err)
			assert.True(deleteRep.GetState().GetSuccess())
		}
	}
}

func (suite *ServerTestSuite) SetupSuite() {
	assert := assert.New(suite.T())

	_ = logcore.InitManager(false)
	cfg := suite.memoryConfig()
	server, err := NewServer(cfg)
	assert.Nil(err)
	assert.NotNil(server)

	go server.Start()

	addr := fmt.Sprintf("%s:%d", cfg.Server.IP, cfg.Server.Port)
	client, err := client.CreateClient([]dfnet.NetAddr{
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
	rpc.StopServer()
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(ServerTestSuite))
}
