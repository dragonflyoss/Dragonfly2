package mgr

import (
	"context"
	"errors"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	logger "github.com/dragonflyoss/Dragonfly2/pkg/dflog"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/scheduler/config"
	"hash/crc32"

	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/cdnsystem/client"
	"github.com/dragonflyoss/Dragonfly2/scheduler/types"
)

const TinyFileSize = 128

type CDNManager struct {
	cdnList    []*CDNClient
	cdnInfoMap map[string]*config.CdnServerConfig
}

func createCDNManager() *CDNManager {
	cdnMgr := &CDNManager{
		cdnInfoMap: make(map[string]*config.CdnServerConfig),
	}
	return cdnMgr
}

func (cm *CDNManager) InitCDNClient() {
	list := config.GetConfig().CDN.List
	for _, cdns := range list {
		if len(cdns) < 1 {
			continue
		}
		var addrs []basic.NetAddr
		for i, cdn := range cdns {
			addrs = append(addrs, basic.NetAddr{
				Type: basic.TCP,
				Addr: fmt.Sprintf("%s:%d", cdn.IP, cdn.RpcPort),
			})
			cm.cdnInfoMap[cdn.CdnName] = &cdns[i]
		}
		seederClient, err := client.CreateClient(addrs)
		if err != nil {
			logger.Errorf("create cdn client failed main addr [%s]", addrs[0])
		}
		cm.cdnList = append(cm.cdnList, &CDNClient{SeederClient: seederClient, mgr: cm})
	}
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

func (cm *CDNManager) getCdnInfo(seederName string) *config.CdnServerConfig {
	return cm.cdnInfoMap[seederName]
}

type CDNClient struct {
	client.SeederClient
	mgr *CDNManager
}

func (c *CDNClient) Work(task *types.Task, ch <-chan *cdnsystem.PieceSeed) {
	for {
		select {
		case ps, ok := <-ch:
			if !ok {
				break
			} else if ps != nil {
				pieceNum := int32(-1)
				if ps.PieceInfo != nil {
					pieceNum = ps.PieceInfo.PieceNum
				}
				logger.Debugf("receive a pieceSeed from cdn: taskId[%s]-%d done [%v]", task.TaskId, pieceNum, ps.Done)
				c.processPieceSeed(task, ps)
			}
		}
	}
}

func (c *CDNClient) processPieceSeed(task *types.Task, ps *cdnsystem.PieceSeed) (err error) {
	hostId := c.getHostUuid(ps)
	host, ok := GetHostManager().GetHost(hostId)
	if !ok {
		ip, rpcPort, downPort := "", 0, 0
		cdnInfo := c.mgr.getCdnInfo(ps.SeederName)
		if cdnInfo != nil {
			ip, rpcPort, downPort = cdnInfo.IP, cdnInfo.RpcPort, cdnInfo.DownloadPort
		}
		host = &types.Host{
			Type: types.HostTypeCdn,
			PeerHost: scheduler.PeerHost{
				Uuid:     hostId,
				HostName: hostId,
				Ip:       ip,
				RpcPort:  int32(rpcPort),
				DownPort: int32(downPort),
			},
		}
		host = GetHostManager().AddHost(host)
	}
	pid := ps.PeerId
	peerTask, _ := GetPeerTaskManager().GetPeerTask(pid)
	if peerTask == nil {
		peerTask = GetPeerTaskManager().AddPeerTask(pid, task, host)
	} else if peerTask.Host == nil {
		peerTask.Host = host
	}

	if ps.Done {
		task.PieceTotal = peerTask.GetFinishedNum()
		task.ContentLength = ps.ContentLength
		peerTask.Success = true

		//
		if task.PieceTotal == 1 {
			if task.ContentLength <= TinyFileSize {
				content, er := c.getTinyFileContent(task)
				if er == nil && len(content) == int(task.ContentLength) {
					task.SizeScope = base.SizeScope_TINY
					task.DirectPiece = &scheduler.RegisterResult_PieceContent{
						PieceContent: content,
					}
					return
				}
			}
			// other wise scheduler as a small file
			task.SizeScope = base.SizeScope_SMALL
			return
		}

		task.SizeScope = base.SizeScope_NORMAL
		return
	}

	task.AddPiece(c.createPiece(task, ps, peerTask))

	peerTask.AddPieceStatus(&scheduler.PieceResult{
		PieceNum: ps.PieceInfo.PieceNum,
		Success:  true,
		// currently completed piece count
		FinishedCount: ps.PieceInfo.PieceNum + 1,
	})

	return
}

func (c *CDNClient) getHostUuid(ps *cdnsystem.PieceSeed) string {
	return fmt.Sprintf("cdn:%s", ps.PeerId)
}

func (c *CDNClient) createPiece(task *types.Task, ps *cdnsystem.PieceSeed, pt *types.PeerTask) *types.Piece {
	p := task.GetOrCreatePiece(ps.PieceInfo.PieceNum)
	p.PieceInfo = *ps.PieceInfo
	return p
}

func (c *CDNClient) getTinyFileContent(task *types.Task) (content []byte, err error) {
	resp, err := c.GetPieceTasks(context.TODO(), &base.PieceTaskRequest{
		TaskId:   task.TaskId,
		StartNum: 0,
		Limit:    2,
	})
	if err != nil {
		return
	}
	if resp == nil || len(resp.PieceInfos) != 1 || resp.TotalPiece != 1 || resp.ContentLength > TinyFileSize {
		err = errors.New("not a tiny file")
	}

	// TODO download the tiny file

	return
}
