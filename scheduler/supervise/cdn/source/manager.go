/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package source

import (
	"context"

	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/supervise"
)

type manager struct {
	peerManager supervise.PeerMgr
	hostManager supervise.HostMgr
}

func NewManager(peerManager supervise.PeerMgr, hostManager supervise.HostMgr) (supervise.CDNMgr, error) {
	mgr := &manager{
		peerManager: peerManager,
		hostManager: hostManager,
	}
	return mgr, nil
}

func (m manager) OnNotify(data *config.DynconfigData) {
	panic("implement me")
}

func (m manager) StartSeedTask(ctx context.Context, task *supervise.Task) (*supervise.Peer, error) {
	//stream, err := cm.client.ObtainSeeds(context.Background(), &cdnsystem.SeedRequest{
	//	TaskId:  task.TaskID,
	//	Url:     task.URL,
	//	UrlMeta: task.URLMeta,
	//})
	//if err != nil {
	//	if cdnErr, ok := err.(*dferrors.DfError); ok {
	//		logger.Errorf("failed to obtain cdn seed: %v", cdnErr)
	//		switch cdnErr.Code {
	//		case dfcodes.CdnTaskRegistryFail:
	//			return errors.Wrap(cdn.ErrCDNRegisterFail, "obtain seeds")
	//		case dfcodes.CdnTaskDownloadFail:
	//			return errors.Wrapf(cdn.ErrCDNDownloadFail, "obtain seeds")
	//		default:
	//			return errors.Wrapf(cdn.ErrCDNUnknown, "obtain seeds")
	//		}
	//	}
	//	return errors.Wrapf(cdn.ErrCDNInvokeFail, "obtain seeds from cdn: %v", err)
	//}
	//return cm.receivePiece(task, stream)
	//source.GetContentLength(context.Background(), task.URL, nil)
	//task.ListPeers()
	//task.SetStatus(types.TaskStatusSuccess)
	return nil, nil
}

var _ supervise.CDNMgr = (*manager)(nil)
