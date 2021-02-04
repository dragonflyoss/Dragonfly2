package clientutil

import (
	"fmt"
	"os"
	"time"

	"github.com/dragonflyoss/Dragonfly/v2/pkg/rpc/scheduler"
)

func GenPeerID(peerHost *scheduler.PeerHost) string {
	// FIXME review peer id format
	return fmt.Sprintf("%s-%d-%d-%d", peerHost.Ip, peerHost.RpcPort, os.Getpid(), time.Now().UnixNano())
}
