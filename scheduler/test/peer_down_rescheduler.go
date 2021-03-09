package test

import (
	"context"
	"d7y.io/dragonfly/v2/pkg/basic/dfnet"
	"d7y.io/dragonfly/v2/pkg/dfcodes"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	rpccommon "d7y.io/dragonfly/v2/pkg/rpc/base/common"
	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
	schedulerclient "d7y.io/dragonfly/v2/pkg/rpc/scheduler/client"
	"d7y.io/dragonfly/v2/scheduler/test/common"
	"d7y.io/dragonfly/v2/scheduler/test/mock_client"
	testifyassert "github.com/stretchr/testify/assert"
	"strings"
	"time"
)

func (suite *SchedulerTestSuite) Test901CDNDownRescheduleCDN() {
	tl := common.NewE2ELogger()
	assert := testifyassert.New(suite.T())

	var (
		err error
		peer = "cdn_down_peer0"
		task = "unknown"
	)

	peerHost := &scheduler.PeerHost{
		Uuid:           "peer-host-0",
		Ip:             "127.0.0.1",
		RpcPort:        0,
		DownPort:       0,
		HostName:       "localhost",
		SecurityDomain: "",
		Location:       "",
		Idc:            "",
		NetTopology:    "",
	}
	request := &scheduler.PeerTaskRequest{
		Url:         "http://ant:sys@fileshare.glusterfs.svc.eu95.alipay.net/shells/check-ulogfs-logdata.sh",
		Filter:      "",
		BizId:       "biz-0",
		UrlMata:     nil,
		PeerId:      peer,
		PeerHost:    peerHost,
		HostLoad:    nil,
		IsMigrating: false,
	}

	sched, err := schedulerclient.GetClientByAddr([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: "127.0.0.1:8002",
		},
	})
	assert.Nil(err)

	ctx := context.Background()
	result, err := sched.RegisterPeerTask(ctx, request)
	assert.Nil(err)

	task = result.TaskId
	assert.True(result.State.Success)

	pieceResultCh, peerPacketCh, err := sched.ReportPieceResult(ctx, result.TaskId, request)
	assert.Nil(err)

	var peerPacket *scheduler.PeerPacket
	// timeout
	select {
	case peerPacket = <-peerPacketCh:
		assert.True(peerPacket.State.Success)
	case <-time.After(time.Second*5):
		assert.Fail("scheduler failed")
	}

	isCDN := strings.HasSuffix(peerPacket.MainPeer.PeerId, rpccommon.CdnSuffix)
	assert.True(isCDN)
	if isCDN {
		pieceResultCh <- &scheduler.PieceResult{
			TaskId:        task,
			SrcPid:        peer,
			DstPid:        peerPacket.MainPeer.PeerId,
			Success:       false,
			Code:          dfcodes.CdnTaskNotFound,
			HostLoad:      nil,
			FinishedCount: -1,
		}
		select {
			case peerPacket = <-peerPacketCh:
				tl.Logf("%#v", peerPacket)
		case <-time.After(time.Second*5):
			assert.Fail("rescheduler failed")
		}
	}
	tl.Log("bad client test all client download file finished")
}


func (suite *SchedulerTestSuite) Test902CDNDownReschedulePeer() {
	tl := common.NewE2ELogger()
	assert := testifyassert.New(suite.T())

	var (
		err error
		url = "http://dragonfly.com?type=cdn_down_rescheduler_peer"
		peer1 = "cdrp_peer_1"
		peer2 = "cdrp_peer_2"
		task = "unknown"
	)

	for i := 0; i < 4; i++ {
		client := mock_client.NewMockClient("127.0.0.1:8002", url, "cdrp", tl)
		go client.Start()
	}

	peerHost1 := &scheduler.PeerHost{
		Uuid:           "reschedule-peer-host-1",
		Ip:             "127.0.0.1",
		RpcPort:        0,
		DownPort:       0,
		HostName:       "localhost",
		SecurityDomain: "",
		Location:       "",
		Idc:            "",
		NetTopology:    "",
	}
	request1 := &scheduler.PeerTaskRequest{
		Url:         url,
		Filter:      "",
		BizId:       "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "",
			Range: "",
		},
		PeerId:      peer1,
		PeerHost:    peerHost1,
		HostLoad:    nil,
		IsMigrating: false,
	}

	peerHost2 := &scheduler.PeerHost{
		Uuid:           "reschedule-peer-host-2",
		Ip:             "127.0.0.1",
		RpcPort:        0,
		DownPort:       0,
		HostName:       "localhost",
		SecurityDomain: "",
		Location:       "",
		Idc:            "",
		NetTopology:    "",
	}
	request2 := &scheduler.PeerTaskRequest{
		Url:         url,
		Filter:      "",
		BizId:       "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "",
			Range: "",
		},
		PeerId:      peer2,
		PeerHost:    peerHost2,
		HostLoad:    nil,
		IsMigrating: false,
	}

	sched, err := schedulerclient.GetClientByAddr([]dfnet.NetAddr{
		{
			Type: dfnet.TCP,
			Addr: "127.0.0.1:8002",
		},
	})
	assert.Nil(err)


	ctx := context.Background()
	result, err := sched.RegisterPeerTask(ctx, request2)
	assert.Nil(err)
	pieceResultCh, peerPacketCh, err := sched.ReportPieceResult(ctx, result.TaskId, request2)

	pieceResultCh <- scheduler.NewZeroPieceResult(task, peer2)
	time.Sleep(time.Second)

	ctx = context.Background()
	result, err = sched.RegisterPeerTask(ctx, request1)
	assert.Nil(err)

	task = result.TaskId
	assert.True(result.State.Success)

	pieceResultCh, peerPacketCh, err = sched.ReportPieceResult(ctx, result.TaskId, request1)
	assert.Nil(err)

	var peerPacket *scheduler.PeerPacket
	var oldPeerId string
	// timeout
	select {
	case peerPacket = <-peerPacketCh:
		assert.True(peerPacket.State.Success)
		oldPeerId = peerPacket.MainPeer.PeerId
	case <-time.After(time.Second*5):
		assert.Fail("scheduler failed")
	}

	code := dfcodes.ClientPieceTaskRequestFail
	isCDN := strings.HasSuffix(peerPacket.MainPeer.PeerId, rpccommon.CdnSuffix)
	if isCDN {
		code = dfcodes.CdnTaskNotFound
	}
	time.Sleep(time.Second/20)

	pieceResultCh <- &scheduler.PieceResult{
		TaskId:        task,
		SrcPid:        peer1,
		DstPid:        peerPacket.MainPeer.PeerId,
		Success:       false,
		Code:          code,
		HostLoad:      nil,
		FinishedCount: -1,
	}
	select {
	case peerPacket = <-peerPacketCh:
		tl.Logf("%#v", peerPacket)
		assert.Equal(false, peerPacket.MainPeer!=nil && peerPacket.MainPeer.PeerId == oldPeerId, "reschedule the same peer")
	case <-time.After(time.Second*10):
		assert.Fail("rescheduler failed")
	}

	tl.Log("bad client test all client download file finished")
}


