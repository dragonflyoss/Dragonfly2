package training

import (
	"math/rand"
	"testing"
	"time"

	"d7y.io/dragonfly/v2/scheduler/config"

	"d7y.io/dragonfly/v2/scheduler/storage"
)

func TestTraining(t *testing.T) {
	tmp := "/Users/wd/.dragonfly/data"
	sto, _ := storage.New(tmp)
	rand.Seed(time.Now().Unix())

	tests := []struct {
		name    string
		baseDir string
		mock    func(t *testing.T)
	}{
		{
			name:    "random record preprocess",
			baseDir: tmp,
			mock: func(t *testing.T) {
				for i := 0; i < 20000; i++ {
					record := storage.Record{
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
						CreateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
						UpdateAt:       time.Now().Unix()/7200 + rand.Int63n(10),
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
			training := NewLinearTraining(sto, nil, nil, &config.TrainingConfig{})
			training.Process()
		})
	}
}
