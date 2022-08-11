package evaluator

import (
	"bytes"
	"encoding/gob"

	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/scheduler/config"
	"d7y.io/dragonfly/v2/scheduler/resource"
	"d7y.io/dragonfly/v2/scheduler/storage"
	"d7y.io/dragonfly/v2/scheduler/training/models"
	"github.com/gocarina/gocsv"
	"github.com/sjwhitworth/golearn/base"
)

// TimeBucketGap time gap between the prior bucket and the next bucket.
const TimeBucketGap = 7200

type MLEvaluator struct {
	cfg          config.DynconfigInterface
	needVersion  chan uint64
	modelVersion chan *types.ModelVersion
	model        *types.ModelVersion
}

func (mle *MLEvaluator) Evaluate(parent *resource.Peer, peer *resource.Peer, taskPieceCount int32) float64 {
	mle.LoadModel()
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
		ParentCreateAt: parent.CreateAt.Load().UnixNano() / TimeBucketGap,
		ParentUpdateAt: parent.UpdateAt.Load().UnixNano() / TimeBucketGap,
	}
	model, err := decodeModelData(mle.model.Data)
	if err != nil {
		return -1
	}
	str, err := gocsv.MarshalString(record)
	if err != nil {
		return -1
	}
	strReader := bytes.NewReader([]byte(str))
	data, err := base.ParseCSVToInstancesFromReader(strReader, false)
	if err != nil {
		return -1
	}
	out, err := model.Predict(data)
	if err != nil {
		return -1
	}
	attrSpec1, err := out.GetAttribute(out.AllAttributes()[0])
	if err != nil {
		return -1
	}
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
	mle.needVersion <- configData.SchedulerCluster.ID
	model, ok := <-mle.modelVersion
	if ok {
		mle.model = model
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

func decodeModelData(data []byte) (*models.LinearRegression, error) {
	buf := new(bytes.Buffer)
	buf.Write(data)
	dec := gob.NewDecoder(buf)
	var model models.LinearRegression
	err := dec.Decode(&model)
	if err != nil {
		return nil, err
	}
	return &model, nil
}
