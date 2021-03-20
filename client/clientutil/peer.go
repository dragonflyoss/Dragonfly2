package clientutil

import (
	"fmt"
	"os"

	"github.com/pborman/uuid"

	"d7y.io/dragonfly/v2/pkg/rpc/scheduler"
)

func GenPeerID(peerHost *scheduler.PeerHost) string {
	return fmt.Sprintf("%s-%d-%s", peerHost.Ip, os.Getpid(), uuid.New())
}
