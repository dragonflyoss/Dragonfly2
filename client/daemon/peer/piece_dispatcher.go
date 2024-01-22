/*
 *     Copyright 2022 The Dragonfly Authors
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
 *
 */

package peer

import (
	"cmp"
	"errors"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	logger "d7y.io/dragonfly/v2/internal/dflog"
)

type PieceDispatcher interface {
	// Put pieceSynchronizer put piece request into PieceDispatcher
	Put(req *DownloadPieceRequest)
	// Get downloader will get piece request from PieceDispatcher
	Get() (req *DownloadPieceRequest, err error)
	// Report downloader will report piece download result to PieceDispatcher, so PieceDispatcher can score peers
	Report(result *DownloadPieceResult)
	// Close related resources, and not accept Put and Get anymore
	Close()
}

var ErrNoValidPieceTemporarily = errors.New("no valid piece temporarily")

type pieceDispatcher struct {
	// peerRequests hold piece requests of peers. Key is PeerID, value is piece requests
	peerRequests map[string][]*DownloadPieceRequest
	// score hold the score of each peer.
	score map[string]int64
	// downloaded hold the already successfully downloaded piece num
	downloaded map[int32]struct{}
	// sum is the valid num of piece requests. When sum == 0, the consumer will wait until there is a request is putted
	sum         *atomic.Int64
	closed      bool
	cond        *sync.Cond
	lock        *sync.Mutex
	log         *logger.SugaredLoggerOnWith
	randomRatio float64
	// rand is not thread-safe
	rand *rand.Rand
}

var (
	// the lower, the better
	maxScore = int64(0)
	minScore = (60 * time.Second).Nanoseconds()
)

func NewPieceDispatcher(randomRatio float64, log *logger.SugaredLoggerOnWith) PieceDispatcher {
	lock := &sync.Mutex{}
	pd := &pieceDispatcher{
		peerRequests: map[string][]*DownloadPieceRequest{},
		score:        map[string]int64{},
		downloaded:   map[int32]struct{}{},
		sum:          atomic.NewInt64(0),
		closed:       false,
		cond:         sync.NewCond(lock),
		lock:         lock,
		log:          log.With("component", "pieceDispatcher"),
		randomRatio:  randomRatio,
		rand:         rand.New(rand.NewSource(time.Now().Unix())),
	}
	log.Debugf("piece dispatcher created")
	return pd
}

func (p *pieceDispatcher) Put(req *DownloadPieceRequest) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if reqs, ok := p.peerRequests[req.DstPid]; ok {
		p.peerRequests[req.DstPid] = append(reqs, req)
	} else {
		p.peerRequests[req.DstPid] = []*DownloadPieceRequest{req}
	}
	if _, ok := p.score[req.DstPid]; !ok {
		p.score[req.DstPid] = maxScore
	}
	p.sum.Add(1)
	p.cond.Broadcast()
}

func (p *pieceDispatcher) Get() (req *DownloadPieceRequest, err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for p.sum.Load() == 0 && !p.closed {
		p.cond.Wait()
	}
	if p.closed {
		return nil, errors.New("piece dispatcher already closed")
	}
	return p.getDesiredReq()
}

// getDesiredReq return a req according to performance of each dest peer. It is not thread-safe
func (p *pieceDispatcher) getDesiredReq() (*DownloadPieceRequest, error) {
	distPeerIDs := maps.Keys(p.score)
	if p.rand.Float64() < p.randomRatio { //random shuffle with the probability of randomRatio
		p.rand.Shuffle(len(distPeerIDs), func(i, j int) {
			tmp := distPeerIDs[j]
			distPeerIDs[j] = distPeerIDs[i]
			distPeerIDs[i] = tmp
		})
	} else { // sort by score with the probability of (1-randomRatio)
		slices.SortFunc(distPeerIDs, func(p1, p2 string) int { return cmp.Compare(p.score[p1], p.score[p2]) })
	}

	// iterate all peers, until get a valid piece requests
	for _, peer := range distPeerIDs {
		for len(p.peerRequests[peer]) > 0 {
			// choose a random piece request of a peer
			n := p.rand.Intn(len(p.peerRequests[peer]))
			req := p.peerRequests[peer][n]
			p.peerRequests[peer] = append(p.peerRequests[peer][0:n], p.peerRequests[peer][n+1:]...)
			p.sum.Sub(1)
			if _, ok := p.downloaded[req.piece.PieceNum]; ok { //already downloaded, skip
				// p.log.Debugf("skip already downloaded piece , peer: %s, piece:%d", peer, req.piece.PieceNum)
				continue
			}
			// p.log.Debugf("scores :%v, select :%s, piece:%v", p.score, peer, req.piece.PieceNum)
			return req, nil
		}
	}
	return nil, ErrNoValidPieceTemporarily
}

// Report pieceDispatcher will score peer according to the download result reported by downloader
// The score of peer is not determined only by last piece downloaded, it is smoothed.
func (p *pieceDispatcher) Report(result *DownloadPieceResult) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if result == nil || result.DstPeerID == "" {
		return
	}
	lastScore := p.score[result.DstPeerID]
	if result.Fail {
		p.score[result.DstPeerID] = (lastScore + minScore) / 2
	} else {
		if result.pieceInfo != nil {
			p.downloaded[result.pieceInfo.PieceNum] = struct{}{}
		}
		p.score[result.DstPeerID] = (lastScore + result.FinishTime - result.BeginTime) / 2
	}
	return
}

func (p *pieceDispatcher) Close() {
	p.lock.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.log.Debugf("piece dispatcher closed")
	p.lock.Unlock()
}
