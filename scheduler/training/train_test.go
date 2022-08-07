package training

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/scheduler/storage"
)

func TestTraining_Process(t *testing.T) {
	sto, _ := storage.New(os.TempDir())
	rand.Seed(time.Now().Unix())

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T)
	}{
		{
			name:    "random record preprocess",
			baseDir: os.TempDir(),
			mock: func(t *testing.T) {
				for i := 0; i < 10100; i++ {
					record := storage.Record{
						ID:             "1",
						IP:             rand.Intn(100)%2 + 1,
						HostName:       rand.Intn(100)%2 + 1,
						Tag:            rand.Intn(100)%2 + 1,
						Rate:           float64(rand.Intn(300) + 10),
						ParentPiece:    float64(rand.Intn(240) + 14),
						SecurityDomain: rand.Intn(100)%2 + 1,
						IDC:            rand.Intn(100)%2 + 1,
						NetTopology:    rand.Intn(100)%2 + 1,
						Location:       rand.Intn(100)%2 + 1,
						UploadRate:     float64(rand.Intn(550) + 3),
						State:          rand.Intn(4),
						CreateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
						UpdateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
						ParentID:       strconv.Itoa(rand.Intn(5)),
						ParentCreateAt: time.Now().Unix()/7200 + rand.Int63n(10),
						ParentUpdateAt: time.Now().Unix()/7200 + rand.Int63n(10),
					}
					err := sto.Create(record)
					if err != nil {
						t.Fatal(err)
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.mock(t)
			Process("/var/folders/lw/9tytbm2s6p7037gcnrwvzxlw0000gn/T", DefaultRecord)
		})
	}
}
