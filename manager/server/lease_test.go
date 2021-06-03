package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/manager/config"
	"d7y.io/dragonfly/v2/manager/lease"
	"d7y.io/dragonfly/v2/manager/store/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LeaseTestSuite struct {
	suite.Suite
	lessor lease.Lessor
}

func (suite *LeaseTestSuite) TestGrantSuccess() {
	assert := assert.New(suite.T())

	randStr := suite.randPrefix()
	id1, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.Nil(err)
	assert.NotEmpty(id1)

	id2, err := suite.lessor.Grant(context.TODO(), randStr+"key2", randStr+"value2", 20)
	assert.Nil(err)
	assert.NotEmpty(id2)

	if len(id1) > 0 {
		suite.lessor.Revoke(context.TODO(), id1)
	}

	if len(id2) > 0 {
		suite.lessor.Revoke(context.TODO(), id2)
	}
}

func (suite *LeaseTestSuite) TestGrantFailed() {
	assert := assert.New(suite.T())

	randStr := suite.randPrefix()
	id1, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.Nil(err)
	assert.NotEmpty(id1)

	id2, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 20)
	assert.NotNil(err)
	assert.Empty(id2)

	time.Sleep(time.Duration(5) * time.Second)
	id3, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 20)
	assert.NotNil(err)
	assert.Empty(id3)

	time.Sleep(time.Duration(6) * time.Second)
	id4, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 20)
	assert.Nil(err)
	assert.NotEmpty(id4)

	if len(id1) > 0 {
		suite.lessor.Revoke(context.TODO(), id1)
	}

	if len(id2) > 0 {
		suite.lessor.Revoke(context.TODO(), id2)
	}

	if len(id3) > 0 {
		suite.lessor.Revoke(context.TODO(), id3)
	}

	if len(id4) > 0 {
		suite.lessor.Revoke(context.TODO(), id4)
	}
}

func (suite *LeaseTestSuite) TestKeepAliveSuccess() {
	assert := assert.New(suite.T())

	randStr := suite.randPrefix()
	id1, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.Nil(err)
	assert.NotEmpty(id1)

	if len(id1) > 0 {
		ch, err := suite.lessor.KeepAlive(context.TODO(), id1)
		assert.Nil(err)
		assert.NotNil(ch)
	}

	time.Sleep(time.Duration(15) * time.Second)
	id2, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.NotNil(err)
	assert.Empty(id2)

	if len(id1) > 0 {
		suite.lessor.Revoke(context.TODO(), id1)
	}

	if len(id2) > 0 {
		suite.lessor.Revoke(context.TODO(), id2)
	}
}

func (suite *LeaseTestSuite) TestKeepAliveTimeout() {
	assert := assert.New(suite.T())

	randStr := suite.randPrefix()
	id1, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.Nil(err)
	assert.NotEmpty(id1)

	if len(id1) > 0 {
		ch, err := suite.lessor.KeepAlive(context.TODO(), id1)
		assert.Nil(err)
		assert.NotNil(ch)
	}

	time.Sleep(time.Duration(15) * time.Second)
	id2, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.NotNil(err)
	assert.Empty(id2)

	suite.lessor.Close()
	defer func() {
		suite.lessor = nil
	}()

	time.Sleep(time.Duration(15) * time.Second)
	id3, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.Nil(err)
	assert.NotEmpty(id3)

	if len(id1) > 0 {
		suite.lessor.Revoke(context.TODO(), id1)
	}

	if len(id2) > 0 {
		suite.lessor.Revoke(context.TODO(), id2)
	}

	if len(id3) > 0 {
		suite.lessor.Revoke(context.TODO(), id2)
	}
}

func (suite *LeaseTestSuite) TestKeepAliveMeetRevoke() {
	assert := assert.New(suite.T())

	randStr := suite.randPrefix()
	id1, err := suite.lessor.Grant(context.TODO(), randStr+"key1", randStr+"value1", 10)
	assert.Nil(err)
	assert.NotEmpty(id1)

	ch, err := suite.lessor.KeepAlive(context.TODO(), id1)
	assert.Nil(err)
	assert.NotNil(ch)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ch chan struct{}) {
		defer wg.Done()
		for {
			select {
			case <-ch:
				return
			}
		}
	}(ch)

	time.Sleep(time.Duration(15) * time.Second)
	suite.lessor.Revoke(context.TODO(), id1)

	wg.Wait()
}

func (suite *LeaseTestSuite) randPrefix() string {
	return fmt.Sprintf("%d_", time.Now().Unix())
}

func (suite *LeaseTestSuite) SetupTest() {
	assert := assert.New(suite.T())

	cfg := config.New()
	store, err := client.NewStore(cfg)
	assert.Nil(err)
	assert.NotNil(store)

	suite.lessor, err = lease.NewLessor(store)
	assert.Nil(err)
}

func (suite *LeaseTestSuite) TearDownTest() {
	if suite.lessor != nil {
		suite.lessor.Close()
	}
}

func TestLeaseTestSuite(t *testing.T) {
	suite.Run(t, new(LeaseTestSuite))
}
