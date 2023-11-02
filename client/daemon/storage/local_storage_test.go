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

package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	commonv1 "d7y.io/api/v2/pkg/apis/common/v1"

	"d7y.io/dragonfly/v2/client/config"
	"d7y.io/dragonfly/v2/client/daemon/test"
	clientutil "d7y.io/dragonfly/v2/client/util"
	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/internal/util"
	"d7y.io/dragonfly/v2/pkg/digest"
	"d7y.io/dragonfly/v2/pkg/net/http"
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
)

func TestLocalTaskStore_PutAndGetPiece(t *testing.T) {
	assert := testifyassert.New(t)
	testBytes, err := os.ReadFile(test.File)
	assert.Nil(err, "load test file")
	md5Test, _ := calcFileMd5(test.File, nil)

	dst := path.Join(test.DataDir, taskData+".copy")
	defer os.Remove(dst)

	testCases := []struct {
		name     string
		strategy config.StoreStrategy
		create   func(s *storageManager, taskID, peerID string) (TaskStorageDriver, error)
	}{
		{
			name:     "normal",
			strategy: config.SimpleLocalTaskStoreStrategy,
			create: func(s *storageManager, taskID, peerID string) (TaskStorageDriver, error) {
				return s.CreateTask(
					&RegisterTaskRequest{
						PeerTaskMetadata: PeerTaskMetadata{
							PeerID: peerID,
							TaskID: taskID,
						},
						DesiredLocation: dst,
						ContentLength:   int64(len(testBytes)),
					})
			},
		},
		{
			name:     "normal",
			strategy: config.AdvanceLocalTaskStoreStrategy,
			create: func(s *storageManager, taskID, peerID string) (TaskStorageDriver, error) {
				return s.CreateTask(
					&RegisterTaskRequest{
						PeerTaskMetadata: PeerTaskMetadata{
							PeerID: peerID,
							TaskID: taskID,
						},
						DesiredLocation: dst,
						ContentLength:   int64(len(testBytes)),
					})
			},
		},
		{
			name:     "subtask",
			strategy: config.AdvanceLocalTaskStoreStrategy,
			create: func(s *storageManager, taskID, peerID string) (TaskStorageDriver, error) {
				var (
					parentPeerID = peerID + "-parent"
					parentTaskID = taskID + "-parent"
				)

				_, err := s.CreateTask(
					&RegisterTaskRequest{
						PeerTaskMetadata: PeerTaskMetadata{
							PeerID: parentPeerID,
							TaskID: parentTaskID,
						},
						DesiredLocation: dst,
						ContentLength:   int64(len(testBytes)),
					})
				assert.Nil(err)

				return s.RegisterSubTask(
					context.Background(),
					&RegisterSubTaskRequest{
						Parent: PeerTaskMetadata{
							PeerID: parentPeerID,
							TaskID: parentTaskID,
						},
						SubTask: PeerTaskMetadata{
							PeerID: peerID,
							TaskID: taskID,
						},
						Range: &http.Range{
							Start:  100,
							Length: int64(len(testBytes)),
						},
					})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name+"-"+string(tc.strategy), func(t *testing.T) {
			var (
				taskID    = "task-d4bb1c273a9889fea14abd4651994fe8"
				peerID    = "peer-d4bb1c273a9889fea14abd4651994fe8"
				pieceSize = 512
			)
			sm, err := NewStorageManager(config.SimpleLocalTaskStoreStrategy,
				&config.StorageOption{
					DataPath: test.DataDir,
					TaskExpireTime: clientutil.Duration{
						Duration: time.Minute,
					},
				}, func(request CommonTaskRequest) {
				}, defaultDirectoryMode)
			assert.Nil(err)

			_, err = tc.create(sm.(*storageManager), taskID, peerID)
			assert.Nil(err, "create task storage")

			ts, ok := sm.(*storageManager).LoadTask(PeerTaskMetadata{
				PeerID: peerID,
				TaskID: taskID,
			})
			assert.True(ok, "load created task")

			var pieces []struct {
				index int
				start int
				end   int // not contain in data
			}
			var piecesMd5 []string
			for i := 0; i*pieceSize < len(testBytes); i++ {
				start := i * pieceSize
				end := start + pieceSize
				if end > len(testBytes) {
					end = len(testBytes)
				}
				pieces = append(pieces, struct {
					index int
					start int
					end   int
				}{
					index: i,
					start: start,
					end:   end,
				})
				piecesMd5 = append(piecesMd5, calcPieceMd5(testBytes[start:end]))
			}
			rand.Seed(time.Now().UnixNano())
			rand.Shuffle(len(pieces), func(i, j int) { pieces[i], pieces[j] = pieces[j], pieces[i] })

			// random put all pieces
			for _, p := range pieces {
				_, err = ts.WritePiece(context.Background(), &WritePieceRequest{
					PeerTaskMetadata: PeerTaskMetadata{
						TaskID: taskID,
					},
					PieceMetadata: PieceMetadata{
						Num:    int32(p.index),
						Md5:    piecesMd5[p.index],
						Offset: uint64(p.start),
						Range: http.Range{
							Start:  int64(p.start),
							Length: int64(p.end - p.start),
						},
						Style: commonv1.PieceStyle_PLAIN,
					},
					Reader: bytes.NewBuffer(testBytes[p.start:p.end]),
				})
				assert.Nil(err, "put piece")
			}

			if lts, ok := ts.(*localTaskStore); ok {
				md5TaskData, _ := calcFileMd5(path.Join(lts.dataDir, taskData), nil)
				assert.Equal(md5Test, md5TaskData, "md5 must match")
			} else if lsts, ok := ts.(*localSubTaskStore); ok {
				md5TaskData, _ := calcFileMd5(path.Join(lsts.parent.dataDir, taskData), lsts.Range)
				assert.Equal(md5Test, md5TaskData, "md5 must match")
			}

			// shuffle again for get all pieces
			rand.Shuffle(len(pieces), func(i, j int) { pieces[i], pieces[j] = pieces[j], pieces[i] })
			for _, p := range pieces {
				rd, cl, err := ts.ReadPiece(context.Background(), &ReadPieceRequest{
					PeerTaskMetadata: PeerTaskMetadata{
						TaskID: taskID,
					},
					PieceMetadata: PieceMetadata{
						Num:    int32(p.index),
						Md5:    piecesMd5[p.index],
						Offset: uint64(p.start),
						Range: http.Range{
							Start:  int64(p.start),
							Length: int64(p.end - p.start),
						},
						Style: commonv1.PieceStyle_PLAIN,
					},
				})
				assert.Nil(err, "get piece reader should be ok")
				data, err := io.ReadAll(rd)
				cl.Close()
				assert.Nil(err, "read piece should be ok")
				assert.Equal(p.end-p.start, len(data), "piece length should match")
				assert.Equal(testBytes[p.start:p.end], data, "piece data should match")
			}

			rd, err := ts.ReadAllPieces(context.Background(), &ReadAllPiecesRequest{
				PeerTaskMetadata: PeerTaskMetadata{
					TaskID: taskID,
				},
				Range: nil,
			})
			assert.Nil(err, "get all pieces reader should be ok")
			data, err := io.ReadAll(rd)
			assert.Nil(err, "read all pieces should be ok")
			rd.Close()
			assert.Equal(testBytes, data, "all pieces data should match")

			if lts, ok := ts.(*localTaskStore); ok {
				lts.genMetadata(0, &WritePieceRequest{
					NeedGenMetadata: func(n int64) (total int32, length int64, gen bool) {
						return int32(len(pieces)), int64(len(testBytes)), true
					},
				})
				assert.Equal(digest.SHA256FromStrings(piecesMd5...), lts.PieceMd5Sign)

				// clean up test data
				lts.lastAccess.Store(time.Now().Add(-1 * time.Hour).UnixNano())
				ok = lts.CanReclaim()
				assert.True(ok, "task should gc")
				err = lts.Reclaim()
				assert.Nil(err, "task gc")
			} else if lsts, ok := ts.(*localSubTaskStore); ok {
				lsts.genMetadata(0, &WritePieceRequest{
					NeedGenMetadata: func(n int64) (total int32, length int64, gen bool) {
						return int32(len(pieces)), int64(len(testBytes)), true
					},
				})
				assert.Equal(digest.SHA256FromStrings(piecesMd5...), lsts.PieceMd5Sign)

				// keep original offset
				err = lsts.Store(context.Background(),
					&StoreRequest{
						CommonTaskRequest: CommonTaskRequest{
							Destination: dst,
						},
						MetadataOnly:   false,
						StoreDataOnly:  false,
						TotalPieces:    0,
						OriginalOffset: true,
					})
				assert.Nil(err)
				md5Store, err := calcFileMd5(dst, lsts.Range)
				assert.Nil(err)
				assert.Equal(md5Test, md5Store)

				// just ranged data
				err = lsts.Store(context.Background(),
					&StoreRequest{
						CommonTaskRequest: CommonTaskRequest{
							Destination: dst,
						},
						MetadataOnly:   false,
						StoreDataOnly:  false,
						TotalPieces:    0,
						OriginalOffset: false,
					})
				assert.Nil(err)
				md5Store, err = calcFileMd5(dst, nil)
				assert.Nil(err)
				assert.Equal(md5Test, md5Store)

				// clean up test data
				lsts.parent.lastAccess.Store(time.Now().Add(-1 * time.Hour).UnixNano())
				lsts.parent.Done = true

				ok = lsts.CanReclaim()
				assert.True(ok, "sub task should gc")
				err = lsts.Reclaim()
				assert.Nil(err, "sub task gc")

				ok = lsts.parent.CanReclaim()
				assert.True(ok, "parent task should gc")
				err = lsts.parent.Reclaim()
				assert.Nil(err, "parent task gc")
			}
		})
	}
}

func TestLocalTaskStore_StoreTaskData_Simple(t *testing.T) {
	assert := testifyassert.New(t)
	src := path.Join(test.DataDir, taskData)
	dst := path.Join(test.DataDir, taskData+".copy")
	meta := path.Join(test.DataDir, taskData+".meta")
	// prepare test data
	testData := []byte("test data")
	err := os.WriteFile(src, testData, defaultFileMode)
	assert.Nil(err, "prepare test data")
	defer os.Remove(src)
	defer os.Remove(dst)
	defer os.Remove(meta)

	data, err := os.OpenFile(src, os.O_RDWR, defaultFileMode)
	assert.Nil(err, "open test data")
	defer data.Close()

	matadata, err := os.OpenFile(meta, os.O_RDWR|os.O_CREATE, defaultFileMode)
	assert.Nil(err, "open test meta data")
	matadata.Close()
	ts := localTaskStore{
		SugaredLoggerOnWith: logger.With("test", "localTaskStore"),
		persistentMetadata: persistentMetadata{
			TaskID:       "test",
			DataFilePath: src,
		},
		dataDir:          test.DataDir,
		metadataFilePath: meta,
	}
	ts.lastAccess.Store(time.Now().UnixNano())
	err = ts.Store(context.Background(), &StoreRequest{
		CommonTaskRequest: CommonTaskRequest{
			TaskID:      ts.TaskID,
			Destination: dst,
		},
	})
	assert.Nil(err, "store test data")
	bs, err := os.ReadFile(dst)
	assert.Nil(err, "read output test data")
	assert.Equal(testData, bs, "data must match")
}

func calcFileMd5(filePath string, rg *http.Range) (string, error) {
	var md5String string
	file, err := os.Open(filePath)
	if err != nil {
		return md5String, err
	}
	defer file.Close()

	var rd io.Reader = file
	if rg != nil {
		rd = io.LimitReader(file, rg.Length)
		_, err = file.Seek(rg.Start, io.SeekStart)
		if err != nil {
			return "", err
		}
	}

	hash := md5.New()
	if _, err := io.Copy(hash, rd); err != nil {
		return md5String, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	md5String = hex.EncodeToString(hashInBytes)
	return md5String, nil
}

func calcPieceMd5(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil)[:16])
}

func Test_computePiecePosition(t *testing.T) {
	var testCases = []struct {
		name  string
		total int64
		rg    *http.Range
		start int32
		end   int32
		piece uint32
	}{
		{
			name:  "0",
			total: 500,
			rg: &http.Range{
				Start:  0,
				Length: 10,
			},
			start: 0,
			end:   0,
			piece: 100,
		},
		{
			name:  "1",
			total: 500,
			rg: &http.Range{
				Start:  30,
				Length: 60,
			},
			start: 0,
			end:   0,
			piece: 100,
		},
		{
			name:  "2",
			total: 500,
			rg: &http.Range{
				Start:  30,
				Length: 130,
			},
			start: 0,
			end:   1,
			piece: 100,
		},
		{
			name:  "3",
			total: 500,
			rg: &http.Range{
				Start:  350,
				Length: 100,
			},
			start: 3,
			end:   4,
			piece: 100,
		},
		{
			name:  "4",
			total: 500,
			rg: &http.Range{
				Start:  400,
				Length: 100,
			},
			start: 4,
			end:   4,
			piece: 100,
		},
		{
			name:  "5",
			total: 500,
			rg: &http.Range{
				Start:  0,
				Length: 500,
			},
			start: 0,
			end:   4,
			piece: 100,
		},
	}

	assert := testifyassert.New(t)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := computePiecePosition(tc.total, tc.rg, func(length int64) uint32 {
				return tc.piece
			})
			assert.Equal(tc.start, start)
			assert.Equal(tc.end, end)
		})
	}
}

func TestLocalTaskStore_partialCompleted(t *testing.T) {
	var testCases = []struct {
		name            string
		ContentLength   int64
		ReadyPieceCount int32
		Range           http.Range
		Found           bool
	}{
		{
			name:            "range bytes=x-y partial completed",
			ContentLength:   1024,
			ReadyPieceCount: 1,
			Range: http.Range{
				Start:  1,
				Length: 1023,
			},
			Found: true,
		},
		{
			name:            "range bytes=x-y no partial completed",
			ContentLength:   util.DefaultPieceSize * 10,
			ReadyPieceCount: 1,
			Range: http.Range{
				Start:  1,
				Length: util.DefaultPieceSize * 2,
			},
			Found: false,
		},
		{
			name:            "range bytes=x- no partial completed",
			ContentLength:   util.DefaultPieceSizeLimit * 1,
			ReadyPieceCount: 1,
			Range: http.Range{
				Start:  1,
				Length: math.MaxInt - 1,
			},
			Found: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)
			lts := &localTaskStore{
				persistentMetadata: persistentMetadata{
					ContentLength: tc.ContentLength,
					Pieces:        map[int32]PieceMetadata{},
				},
			}
			for i := int32(0); i < tc.ReadyPieceCount; i++ {
				lts.Pieces[i] = PieceMetadata{}
			}
			ok := lts.partialCompleted(&tc.Range)
			assert.Equal(tc.Found, ok)
		})
	}
}

func TestLocalTaskStore_CanReclaim(t *testing.T) {
	testCases := []struct {
		name   string
		lts    *localTaskStore
		expect bool
	}{
		{
			name:   "normal task",
			lts:    &localTaskStore{},
			expect: false,
		},
		{
			name: "invalid task",
			lts: &localTaskStore{
				invalid: *atomic.NewBool(true),
			},
			expect: true,
		},
		{
			name: "never expire task",
			lts: &localTaskStore{
				expireTime: 0,
			},
			expect: false,
		},
		{
			name: "expired task",
			lts: &localTaskStore{
				expireTime: time.Second,
				lastAccess: *atomic.NewInt64(1),
			},
			expect: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testifyassert.New(t)
			assert.Equal(tc.lts.CanReclaim(), tc.expect)
		})
	}
}
