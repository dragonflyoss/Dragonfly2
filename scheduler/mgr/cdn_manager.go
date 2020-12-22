package mgr

import (
	"context"
	"fmt"
	"hash/crc32"

	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem/client"
	"github.com/dragonflyoss/Dragonfly2/pkg/util/net"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

type CDNManager struct {
	cdnList []*CDNClient
}

func CreateCDNManager() *CDNManager {
	cdnMgr := &CDNManager{}
	list := config.GetConfig().CDN.List
	for _, cdns := range list {
		if len(cdns) < 1 {
			continue
		}
		var addrs []basic.NetAddr
		for _, cdn := range cdns {
			addrs = append(addrs, basic.NetAddr{
				Type: cdn.Type,
				Addr: cdn.Addr,
			})
		}
		seederClient, err := client.CreateClient(addrs)
		if err != nil {
			logger.Errorf("create cdn client failed main addr [%s]", addrs[0])
		}
		cdnMgr.cdnList = append(cdnMgr.cdnList, &CDNClient{SeederClient: seederClient})
	}

	return cdnMgr
}

func (cm *CDNManager) TriggerTask(task *types.Task) (err error) {
	cli, err := cm.getCDNClient(task)
	if err != nil {
		return
	}
	seedChan, err := cli.ObtainSeeds(context.TODO(), &cdnsystem.SeedRequest{
		TaskId:  task.TaskId,
		Url:     task.Url,
		Filter:  task.Filter,
		UrlMeta: task.UrlMata,
	})
	if err != nil {
		return
	}

	go cli.Work(task, seedChan)

	return
}

func (cm *CDNManager) getCDNClient(task *types.Task) (cli *CDNClient, err error) {
	if len(cm.cdnList) < 1 {
		return
	}
	pos := crc32.ChecksumIEEE([]byte(task.Url)) % uint32(len(cm.cdnList))
	cli = cm.cdnList[int(pos)]
	return
}

type CDNClient struct {
	client.SeederClient
}

func (c *CDNClient) Work(task *types.Task, ch <-chan *cdnsystem.PieceSeed) {
	for ps := range ch {
		c.processPieceSeed(task, ps)
	}
}

func (c *CDNClient) processPieceSeed(task *types.Task, ps *cdnsystem.PieceSeed) (err error) {
	hostId := c.getHostUuid(ps)
	host, ok := GetHostManager().GetHost(hostId)
	if !ok {
		ip, port, _ := net.ParseAddress(ps.SeedAddr)
		host = &types.Host{
			Type:     types.HostTypeCdn,
			Uuid:     hostId,
			HostName: hostId,
			Ip:       ip,
			Port:     int32(port),
		}
		host = GetHostManager().AddHost(host)
	}
	pid := c.getPeerId(task, ps)
	peerTask, _ := GetPeerTaskManager().GetPeerTask(pid)
	if peerTask == nil {
		peerTask = GetPeerTaskManager().AddPeerTask(pid, task, host)
	}

	if ps.Done {
		peerTask.SetDown()
		return
	}

	task.AddPiece(c.createPiece(task, ps, peerTask))

	peerTask.AddPieceStatus(&types.PieceStatus{
		PieceNum: ps.PieceNum,
		Success:  true,
	})

	return
}

func (c *CDNClient) getPeerId(task *types.Task, ps *cdnsystem.PieceSeed) string {
	return fmt.Sprintf("cdn:%s:%s", ps.SeedAddr, task.TaskId)
}

func (c *CDNClient) getHostUuid(ps *cdnsystem.PieceSeed) string {
	return fmt.Sprintf("cdn:%s", ps.SeedAddr)
}

func (c *CDNClient) createPiece(task *types.Task, ps *cdnsystem.PieceSeed, pt *types.PeerTask) *types.Piece {
	p := &types.Piece{
		PieceNum:    ps.PieceNum,
		PieceRange:  ps.PieceRange,
		PieceMd5:    ps.PieceMd5,
		PieceOffset: ps.PieceOffset,
		PieceStyle:  ps.PieceStyle,
		Task:        task,
	}
	p.AddReadyPeerTask(pt)
	return p
}
