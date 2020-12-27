package client

import (
	"context"
	"fmt"
	"github.com/dragonflyoss/Dragonfly2/pkg/basic"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/log"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/base"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler"
	"github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/client"
	_ "github.com/dragonflyoss/Dragonfly2/pkg/rpc/scheduler/server"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	pid := "002"
	c, err := client.CreateClient([]basic.NetAddr{{Type: basic.TCP, Addr: "localhost:8002"}})
	if err != nil {
		panic(err)
	}

	request := &scheduler.PeerTaskRequest{
		Url:    "http://www.baidu.com",
		Filter: "",
		BizId:  "12345",
		UrlMata: &base.UrlMeta{
			Md5:   "12233",
			Range: "",
		},
		Pid: pid,
		PeerHost: &scheduler.PeerHost{
			Uuid:           "host001",
			Ip:             "127.0.0.1",
			Port:           23456,
			HostName:       "host001",
			SecurityDomain: "",
			Location:       "",
			Idc:            "",
			Switch:         "",
		},
	}
	replyChan := make(chan *scheduler.PiecePackage_PieceTask, 1000)
	pkg, err := c.RegisterPeerTask(context.TODO(), request)
	if err != nil {
		t.Error(err)
		return
	}

	if pkg != nil && len(pkg.PieceTasks) > 0 {
		fmt.Printf("RegisterPeerTask, recieve a pkg: %d, pieceNum:", len(pkg.PieceTasks))
		for _, piece := range pkg.PieceTasks {
			replyChan <- piece
			fmt.Printf("[%d]", piece.PieceNum)
		}
		fmt.Println("")
	} else {
		fmt.Printf("RegisterPeerTask, pkg length: %d\n", len(pkg.PieceTasks))
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	in, out, err := c.PullPieceTasks(ctx, pkg.TaskId, request)
	if err != nil {
		t.Error(err)
		return
	}
	pr := &scheduler.PieceResult{
		TaskId:   pkg.TaskId,
		SrcPid:   pid,
		PieceNum: -1,
	}
	fmt.Println("send first pr")
	in <- pr

	go func() {
		for t := range replyChan {
			pr := &scheduler.PieceResult{
				TaskId:     pkg.TaskId,
				SrcPid:     pid,
				DstPid:     t.DstPid,
				PieceNum:   t.PieceNum,
				PieceRange: t.PieceRange,
				Success:    true,
				ErrorCode:  base.Code_SUCCESS,
				Cost:       10,
			}
			in <- pr
		}
	} ()

	fmt.Println("send first pr finished")
	for {
		resp := <-out
		if len(resp.PieceTasks)>0 {
			fmt.Printf("recieve a pkg: %d, pieceNum:", len(resp.PieceTasks))
			for _, piece := range resp.PieceTasks {
				fmt.Printf("[%d]", piece.PieceNum)
			}
			fmt.Println("")
		} else {
			fmt.Printf("recieve a pkg: %d\n", len(resp.PieceTasks))
		}
		if resp != nil && resp.Done {
			fmt.Println("download finished")
			break
		}
		if resp == nil || len(resp.PieceTasks) == 0 {
			time.Sleep(time.Second)
			go func() {in <- pr}()
			continue
		}
		for _, t := range resp.PieceTasks {
			replyChan <- t
		}
	}

	fmt.Println("client finish")
}
