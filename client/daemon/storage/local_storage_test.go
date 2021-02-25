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
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	testifyassert "github.com/stretchr/testify/assert"

	"d7y.io/dragonfly/v2/client/clientutil"
	"d7y.io/dragonfly/v2/client/daemon/gc"
	"d7y.io/dragonfly/v2/client/daemon/test"
	logger "d7y.io/dragonfly/v2/pkg/dflog"
	"d7y.io/dragonfly/v2/pkg/rpc/base"
	_ "d7y.io/dragonfly/v2/pkg/rpc/dfdaemon/server"
)

func TestMain(m *testing.M) {
	logger.InitDaemon()
	m.Run()
}

func TestLocalTaskStore_PutAndGetPiece_Simple(t *testing.T) {
	assert := testifyassert.New(t)
	testBytes, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	dst := path.Join(test.DataDir, taskData+".copy")
	defer os.Remove(dst)

	var (
		taskID    = "task-d4bb1c273a9889fea14abd4651994fe8"
		peerID    = "peer-d4bb1c273a9889fea14abd4651994fe8"
		pieceSize = 512
	)
	sm, err := NewStorageManager(SimpleLocalTaskStoreStrategy, &Option{
		DataPath: test.DataDir,
		TaskExpireTime: clientutil.Duration{
			Duration: time.Minute,
		},
	}, func(request CommonTaskRequest) {
	})
	var s = sm.(*storageManager)

	err = s.CreateTask(
		RegisterTaskRequest{
			CommonTaskRequest: CommonTaskRequest{
				PeerID:      peerID,
				TaskID:      taskID,
				Destination: dst,
			},
			ContentLength: int64(len(testBytes)),
		})
	assert.Nil(err, "create task storage")
	ts, ok := s.LoadTask(PeerTaskMetaData{
		PeerID: peerID,
		TaskID: taskID,
	})
	assert.True(ok, "")

	var pieces []struct {
		index int
		start int
		end   int // not contain in data
	}
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
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(pieces), func(i, j int) { pieces[i], pieces[j] = pieces[j], pieces[i] })

	// random put all pieces
	for _, p := range pieces {
		_, err = ts.WritePiece(context.Background(), &WritePieceRequest{
			PeerTaskMetaData: PeerTaskMetaData{
				TaskID: taskID,
			},
			PieceMetaData: PieceMetaData{
				Num:    int32(p.index),
				Md5:    "",
				Offset: uint64(p.start),
				Range: clientutil.Range{
					Start:  int64(p.start),
					Length: int64(p.end - p.start),
				},
				Style: base.PieceStyle_PLAIN,
			},
			Reader: bytes.NewBuffer(testBytes[p.start:p.end]),
		})
		assert.Nil(err, "put piece")
	}

	md5Test, _ := calcFileMd5(test.File)
	md5TaskData, _ := calcFileMd5(path.Join(ts.(*localTaskStore).dataDir, taskData))
	assert.Equal(md5Test, md5TaskData, "md5 must match")

	// shuffle again for get all pieces
	rand.Shuffle(len(pieces), func(i, j int) { pieces[i], pieces[j] = pieces[j], pieces[i] })
	for _, p := range pieces {
		rd, cl, err := ts.ReadPiece(context.Background(), &ReadPieceRequest{
			PeerTaskMetaData: PeerTaskMetaData{
				TaskID: taskID,
			},
			PieceMetaData: PieceMetaData{
				Num:    int32(p.index),
				Md5:    "",
				Offset: uint64(p.start),
				Range: clientutil.Range{
					Start:  int64(p.start),
					Length: int64(p.end - p.start),
				},
				Style: base.PieceStyle_PLAIN,
			},
		})
		assert.Nil(err, "get piece should be ok")
		data, err := ioutil.ReadAll(rd)
		cl.Close()
		assert.Nil(err, "read piece should be ok")
		assert.Equal(p.end-p.start, len(data), "piece length should match")
		assert.Equal(testBytes[p.start:p.end], data, "piece data should match")
	}

	// clean up test data
	ts.(*localTaskStore).lastAccess = time.Now().Add(-1 * time.Hour)
	ok, err = ts.(gc.GC).TryGC()
	assert.Nil(err, "task gc")
	assert.True(ok, "task should gc")
}

func TestLocalTaskStore_StoreTaskData_Simple(t *testing.T) {
	assert := testifyassert.New(t)
	src := path.Join(test.DataDir, taskData)
	dst := path.Join(test.DataDir, taskData+".copy")
	meta := path.Join(test.DataDir, taskData+".meta")
	// prepare test data
	testData := []byte("test data")
	err := ioutil.WriteFile(src, testData, defaultFileMode)
	assert.Nil(err, "prepare test data")
	defer os.Remove(src)
	defer os.Remove(dst)
	defer os.Remove(meta)

	data, err := os.OpenFile(src, os.O_RDWR, defaultFileMode)
	assert.Nil(err, "open test data")
	defer data.Close()

	matadata, err := os.OpenFile(meta, os.O_RDWR|os.O_CREATE, defaultFileMode)
	assert.Nil(err, "open test meta data")
	defer matadata.Close()
	ts := localTaskStore{
		SugaredLoggerOnWith: logger.With("test", "localTaskStore"),
		persistentMetadata: persistentMetadata{
			TaskID: "test",
		},
		RWMutex:      &sync.RWMutex{},
		dataDir:      test.DataDir,
		metadataFile: matadata,
		lastAccess:   time.Time{},
	}
	err = ts.Store(context.Background(), &StoreRequest{
		CommonTaskRequest: CommonTaskRequest{
			TaskID:      ts.TaskID,
			Destination: dst,
		},
	})
	assert.Nil(err, "store test data")
	bs, err := ioutil.ReadFile(dst)
	assert.Nil(err, "read output test data")
	assert.Equal(testData, bs, "data must match")
}

func TestLocalTaskStore_ReloadPersistentTask_Simple(t *testing.T) {

}

func TestLocalTaskStore_PutAndGetPiece_Advance(t *testing.T) {
	assert := testifyassert.New(t)
	testBytes, err := ioutil.ReadFile(test.File)
	assert.Nil(err, "load test file")

	dst := path.Join(test.DataDir, taskData+".copy")
	dst, _ = filepath.Abs(dst)
	defer os.Remove(dst)

	var (
		taskID    = "task-d4bb1c273a9889fea14abd4651994fe8"
		peerID    = "peer-d4bb1c273a9889fea14abd4651994fe8"
		pieceSize = 512
	)
	sm, err := NewStorageManager(AdvanceLocalTaskStoreStrategy, &Option{
		DataPath: test.DataDir,
		TaskExpireTime: clientutil.Duration{
			Duration: time.Minute,
		},
	}, func(request CommonTaskRequest) {
	})
	var s = sm.(*storageManager)

	err = s.CreateTask(
		RegisterTaskRequest{
			CommonTaskRequest: CommonTaskRequest{
				PeerID:      peerID,
				TaskID:      taskID,
				Destination: dst,
			},
			ContentLength: int64(len(testBytes)),
		})
	assert.Nil(err, "create task storage")
	ts, ok := s.LoadTask(PeerTaskMetaData{
		PeerID: peerID,
		TaskID: taskID,
	})
	assert.True(ok, "")

	var pieces []struct {
		index int
		start int
		end   int // not contain in data
	}
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
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(pieces), func(i, j int) { pieces[i], pieces[j] = pieces[j], pieces[i] })

	// random put all pieces
	for _, p := range pieces {
		_, err = ts.WritePiece(context.Background(), &WritePieceRequest{
			PeerTaskMetaData: PeerTaskMetaData{
				TaskID: taskID,
			},
			PieceMetaData: PieceMetaData{
				Num:    int32(p.index),
				Md5:    "",
				Offset: uint64(p.start),
				Range: clientutil.Range{
					Start:  int64(p.start),
					Length: int64(p.end - p.start),
				},
				Style: base.PieceStyle_PLAIN,
			},
			Reader: bytes.NewBuffer(testBytes[p.start:p.end]),
		})
		assert.Nil(err, "put piece")
	}

	md5Test, _ := calcFileMd5(test.File)
	md5TaskData, _ := calcFileMd5(path.Join(ts.(*localTaskStore).dataDir, taskData))
	assert.Equal(md5Test, md5TaskData, "md5 must match")

	// shuffle again for get all pieces
	rand.Shuffle(len(pieces), func(i, j int) { pieces[i], pieces[j] = pieces[j], pieces[i] })
	for _, p := range pieces {
		rd, cl, err := ts.ReadPiece(context.Background(), &ReadPieceRequest{
			PeerTaskMetaData: PeerTaskMetaData{
				TaskID: taskID,
			},
			PieceMetaData: PieceMetaData{
				Num:    int32(p.index),
				Md5:    "",
				Offset: uint64(p.start),
				Range: clientutil.Range{
					Start:  int64(p.start),
					Length: int64(p.end - p.start),
				},
				Style: base.PieceStyle_PLAIN,
			},
		})
		assert.Nil(err, "get piece should be ok")
		data, err := ioutil.ReadAll(rd)
		cl.Close()
		assert.Nil(err, "read piece should be ok")
		assert.Equal(p.end-p.start, len(data), "piece length should match")
		assert.Equal(testBytes[p.start:p.end], data, "piece data should match")
	}

	// clean up test data
	ts.(*localTaskStore).lastAccess = time.Now().Add(-1 * time.Hour)
	ok, err = ts.(gc.GC).TryGC()
	assert.Nil(err, "task gc")
	assert.True(ok, "task should gc")
}

func TestLocalTaskStore_StoreTaskData_Advance(t *testing.T) {

}

func TestLocalTaskStore_ReloadPersistentTask_Advance(t *testing.T) {

}

func calcFileMd5(filePath string) (string, error) {
	var md5String string
	file, err := os.Open(filePath)
	if err != nil {
		return md5String, err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return md5String, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	md5String = hex.EncodeToString(hashInBytes)
	return md5String, nil
}
