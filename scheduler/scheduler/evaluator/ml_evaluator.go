package evaluator

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/gocarina/gocsv"
	"github.com/sjwhitworth/golearn/base"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
	"d7y.io/dragonfly/v2/scheduler/training"
	"d7y.io/dragonfly/v2/scheduler/training/models"
)

// TimeBucketGap time gap between the prior bucket and the next bucket.
const TimeBucketGap = 7200

type MLEvaluator struct {
	cfg          config.DynconfigInterface
	needVersion  chan uint64
	modelVersion chan *types.ModelVersion
	Model        *types.ModelVersion
	lock         sync.Mutex
}

func (mle *MLEvaluator) Evaluate(parent *resource.Peer, peer *resource.Peer, taskPieceCount int32) float64 {
	mle.lock.Lock()
	defer mle.lock.Unlock()
	mle.LoadModel()
	logger.Infof("MLEvaluator peer id is %v, MLEvaluator parent id is %v", peer.ID, parent.ID)
	record := storage.Record{
		Rate:           0,
		HostType:       int(peer.Host.Type),
		CreateAt:       peer.CreateAt.Load().Unix() / TimeBucketGap,
		UpdateAt:       peer.UpdateAt.Load().Unix() / TimeBucketGap,
		IP:             strFeatureTrans(peer.Host.IP, parent.Host.IP),
		HostName:       strFeatureTrans(peer.Host.Hostname, parent.Host.Hostname),
		Tag:            strFeatureTrans(peer.Tag, parent.Tag),
		ParentPiece:    figureFeatureTrans(float64(taskPieceCount), float64(parent.Pieces.Len())),
		SecurityDomain: strFeatureTrans(peer.Host.SecurityDomain, parent.Host.SecurityDomain),
		IDC:            strFeatureTrans(peer.Host.IDC, parent.Host.IDC),
		NetTopology:    strFeatureTrans(peer.Host.NetTopology, parent.Host.NetTopology),
		Location:       strFeatureTrans(peer.Host.Location, parent.Host.Location),
		UploadRate:     figureFeatureTrans(float64(peer.Host.FreeUploadLoad()), float64(parent.Host.FreeUploadLoad())),
		ParentHostType: int(parent.Host.Type),
		ParentCreateAt: parent.CreateAt.Load().Unix() / TimeBucketGap,
		ParentUpdateAt: parent.UpdateAt.Load().Unix() / TimeBucketGap,
	}
	logger.Infof("compute HostType is %v", int(peer.Host.Type))
	logger.Infof("compute CreateAt is %v", peer.CreateAt.Load().Unix()/TimeBucketGap)
	logger.Infof("compute UpdateAt is %v", peer.UpdateAt.Load().Unix()/TimeBucketGap)
	logger.Infof("compute IP is %v", strFeatureTrans(peer.Host.IP, parent.Host.IP))
	logger.Infof("compute HostName is %v", strFeatureTrans(peer.Host.Hostname, parent.Host.Hostname))
	logger.Infof("compute Tag is %v", strFeatureTrans(peer.Tag, parent.Tag))
	logger.Infof("compute ParentPiece is %v", figureFeatureTrans(float64(taskPieceCount), float64(parent.Pieces.Len())))
	logger.Infof("compute SecurityDomain is %v", strFeatureTrans(peer.Host.SecurityDomain, parent.Host.SecurityDomain))
	logger.Infof("compute IDC is %v", strFeatureTrans(peer.Host.IDC, parent.Host.IDC))
	logger.Infof("compute NetTopology is %v", strFeatureTrans(peer.Host.NetTopology, parent.Host.NetTopology))
	logger.Infof("compute Location is %v", strFeatureTrans(peer.Host.Location, parent.Host.Location))
	logger.Infof("compute UploadRate is %v", figureFeatureTrans(float64(peer.Host.FreeUploadLoad()), float64(parent.Host.FreeUploadLoad())))
	logger.Infof("compute ParentHostType is %v", int(parent.Host.Type))
	logger.Infof("compute ParentCreateAt is %v", parent.CreateAt.Load().Unix()/TimeBucketGap)
	logger.Infof("compute ParentUpdateAt is %v", parent.UpdateAt.Load().Unix()/TimeBucketGap)
	var model models.LinearRegression
	err := json.Unmarshal(mle.Model.Data, &model)
	if err != nil {
		logger.Infof("decode model fail, error is %v", err)
		return -1
	}
	str, err := gocsv.MarshalString([]storage.Record{record})
	if err != nil {
		logger.Infof("marshal model fail, error is %v", err)
		return -1
	}
	str = str[156:]
	strReader := bytes.NewReader([]byte(str))
	data, err := base.ParseCSVToInstancesFromReader(strReader, false)
	if err != nil {
		logger.Infof("ParseCSVToInstancesFromReader model fail, error is %v", err)
		return -1
	}
	err = training.MissingValue(data)
	if err != nil {
		logger.Infof("missingValue model fail, error is %v", err)
		return 0
	}
	err = training.PredictNormalize(data)
	if err != nil {
		logger.Infof("PredictNormalize model fail, error is %v", err)
		return 0
	}
	out, err := model.Predict(data)
	if err != nil {
		logger.Infof("predict model fail, error is %v", err)
		return -1
	}
	attrSpec1, err := out.GetAttribute(out.AllAttributes()[0])
	if err != nil {
		logger.Infof("GetAttribute model fail, error is %v", err)
		return -1
	}
	logger.Infof("score is %v", base.UnpackBytesToFloat(out.Get(attrSpec1, 0)))
	return base.UnpackBytesToFloat(out.Get(attrSpec1, 0))
}

func (mle *MLEvaluator) IsBadNode(peer *resource.Peer) bool {
	return NormalIsBadNode(peer)
}

func (mle *MLEvaluator) LoadModel() {
	configData, err := mle.cfg.Get()
	if err != nil {
		return
	}
	logger.Infof("start to evaluate model, we need watcher to load model, schedulerID is %v", configData.SchedulerCluster.ID)
	mle.needVersion <- configData.SchedulerCluster.ID
	model, ok := <-mle.modelVersion
	if ok {
		logger.Info("load model function success")
		mle.Model = model
	} else {
		logger.Info("fail to execute load model function")
	}
}

func (mle *MLEvaluator) EvalType() string {
	return MLAlgorithm
}

func NewMLEvaluator(dynconfig config.DynconfigInterface, needVersion chan uint64, modelVersion chan *types.ModelVersion) Evaluator {
	return &MLEvaluator{
		cfg:          dynconfig,
		needVersion:  needVersion,
		modelVersion: modelVersion,
	}
}

func strFeatureTrans(str1 string, str2 string) int {
	if str1 == str2 {
		return 0
	}
	return 1
}

func figureFeatureTrans(a float64, b float64) float64 {
	if b == 0 {
		return -1
	}
	return a / b
}
