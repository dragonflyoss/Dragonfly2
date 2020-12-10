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

package daemon

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
	"sync"
	"testing"
	"time"

	"github.com/dragonflyoss/Dragonfly2/client/util"
	"github.com/dragonflyoss/Dragonfly2/pkg/grpc/base"
)

const (
	testFile = "testdata/go.html"
)

func TestLocalTaskStore_PutAndGetPiece(t *testing.T) {
	testBytes, err := ioutil.ReadFile(testFile)
	if err != nil {
		t.Fatalf("load test file failed: %s", err)
	}

	var (
		taskID    = "task-d4bb1c273a9889fea14abd4651994fe8"
		pieceSize = 512
	)
	ts, err := NewLocalTaskStorageBuilder(taskID, &StorageOption{
		BaseDir:    "testdata",
		GCInterval: time.Minute,
	})
	if err != nil {
		t.Fatalf("create task storage failed: %s", err)
	}

	var pieces []struct {
		index int
		start int
		end   int
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
		err = ts.PutPiece(context.Background(), &PutPieceRequest{
			PieceMetaData: PieceMetaData{
				TaskID: taskID,
				Num:    int32(p.index),
				Md5:    "",
				Offset: uint64(p.start),
				Range: util.Range{
					Start:  int64(p.start),
					Length: int64(p.end - p.start),
				},
				Style: base.PieceStyle_PLAIN_UNSPECIFIED,
			},
			Reader: bytes.NewBuffer(testBytes[p.start:p.end]),
		})
		if err != nil {
			t.Fatalf("put piece failed: %s", err)
		}
	}

	md5Test, _ := getFileMd5(testFile)
	md5TaskData, _ := getFileMd5(path.Join(ts.(*localTaskStore).dir, taskData))
	if md5Test != md5TaskData {
		t.Error("md5 not match")
	}

	// shuffle again for get all pieces
	rand.Shuffle(len(pieces), func(i, j int) { pieces[i], pieces[j] = pieces[j], pieces[i] })
	for _, p := range pieces {
		rd, cl, err := ts.GetPiece(context.Background(), &GetPieceRequest{
			TaskID: taskID,
			Num:    int32(p.index),
			Md5:    "",
			Offset: uint64(p.start),
			Range: util.Range{
				Start:  int64(p.start),
				Length: int64(p.end - p.start),
			},
			// TODO test more styles
			Style: base.PieceStyle_PLAIN_UNSPECIFIED,
		})
		if err != nil {
			t.Fatalf("get piece failed: %s", err)
		}
		data, err := ioutil.ReadAll(rd)
		cl.Close()
		if err != nil {
			t.Fatalf("read piece failed: %s", err)
		} else if len(data) != (p.end - p.start) {
			t.Fatalf("piece length not match")
		}
		if bytes.Compare(data, testBytes[p.start:p.end]) != 0 {
			t.Fatalf("piece data not match")
		}
	}

	// clean up test data
	ts.(*localTaskStore).lastAccess = time.Now().Add(-1 * time.Hour)
	if gc, err := ts.(GC).TryGC(); err != nil {
		t.Fatalf("gc task failed: %s", err)
	} else if !gc {
		t.Fatalf("task should gc but not")
	}
}

func TestLocalTaskStore_StoreTaskData(t *testing.T) {
	src := path.Join("testdata", taskData)
	dst := path.Join("testdata", taskData+".copy")
	// prepare test data
	err := ioutil.WriteFile(src, []byte("test data"), defaultFileMode)
	if err != nil {
		t.Fatalf("prepare test data failed: %s", err)
	}
	defer os.Remove(src)
	defer os.Remove(dst)

	data, err := os.OpenFile(src, os.O_RDWR, defaultFileMode)
	if err != nil {
		t.Fatalf("open test data failed: %s", err)
	}
	defer data.Close()
	ts := localTaskStore{
		lock:     &sync.Mutex{},
		taskID:   "test",
		dir:      "testdata",
		metadata: nil,
		data:     data,
	}
	err = ts.StoreTaskData(context.Background(), &StoreRequest{
		TaskID:      ts.taskID,
		Destination: dst,
	})
	if err != nil {
		t.Fatalf("open test data failed: %s", err)
	}
	bs, err := ioutil.ReadFile(dst)
	if err != nil {
		t.Fatalf("open dst test data failed: %s", err)
	}
	if string(bs) != "test data" {
		t.Errorf("data not match")
	}
}

func getFileMd5(filePath string) (string, error) {
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
