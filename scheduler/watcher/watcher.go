package watcher

import (
	"context"
	"encoding/json"
	"sort"

	managerv1 "d7y.io/api/pkg/apis/manager/v1"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/rpc/manager/client"
	"d7y.io/dragonfly/v2/scheduler/training/models"
)

type Watcher struct {
	needVersion       chan uint64
	modelVersion      chan *types.ModelVersion
	done              chan struct{}
	mc                client.Client
	standard          *types.ModelVersion
	EnableAutoRefresh bool
}

func (w *Watcher) Serve() {
	go func() {
		for {
			select {
			case schID := <-w.needVersion:
				logger.Info("watcher processing ...")
				if schID <= 0 {
					w.modelVersion <- nil
					continue
				}
				logger.Infof("schID is %v", schID)
				if w.EnableAutoRefresh {
					logger.Info("EnableAutoRefresh")
					// Set by user
					model, err := w.mc.GetModel(context.Background(), &managerv1.GetModelRequest{
						ModelId:     types.ModelIDEvaluator,
						SchedulerId: schID,
					})
					if err != nil {
						logger.Info("get model fail, error is %v", err)
						w.modelVersion <- nil
						continue
					}
					version, err := w.mc.GetModelVersion(context.Background(), &managerv1.GetModelVersionRequest{
						SchedulerId: schID,
						ModelId:     types.ModelIDEvaluator,
						VersionId:   model.VersionId,
					})
					if err != nil {
						logger.Infof("get modelVersion fail, error is %v", err)
						w.modelVersion <- nil
						continue
					}
					var data models.LinearRegression
					err = json.Unmarshal(version.Data, &data)
					if err != nil {
						logger.Infof("decode error, error is %v", err)
						return
					}
					w.modelVersion <- &types.ModelVersion{
						Data: version.Data,
						MAE:  version.Mae,
						MSE:  version.Mse,
						RMSE: version.Rmse,
						R2:   version.R2,
					}

				} else {
					versions, err := w.mc.ListModelVersions(context.Background(), &managerv1.ListModelVersionsRequest{
						ModelId:     types.ModelIDEvaluator,
						SchedulerId: schID,
					})
					if err != nil {
						logger.Info("list modelVersion fail")
						w.modelVersion <- nil
						continue
					}
					sort.Slice(versions.ModelVersions, func(i, j int) bool {
						return versions.ModelVersions[i].UpdatedAt.Seconds > versions.ModelVersions[j].UpdatedAt.Seconds
					})
					flag := false
					for _, version := range versions.ModelVersions {
						if (w.standard != nil && w.satisfyStandard(version)) || (w.standard == nil) {
							w.modelVersion <- &types.ModelVersion{
								Data: version.Data,
								MAE:  version.Mae,
								MSE:  version.Mse,
								RMSE: version.Rmse,
								R2:   version.R2,
							}
							logger.Infof("AutoRefresh closed, model version is %v, model is %v", version.VersionId, version.Data)
							flag = true
							break
						}
					}

					if !flag {
						logger.Info("AutoRefresh closed, no modelVersion passed")
						w.modelVersion <- nil
					}
				}
			case <-w.done:
				return
			}
		}
	}()
}

func (w *Watcher) satisfyStandard(version *managerv1.ModelVersion) bool {
	if version.Mae < w.standard.MAE || version.Mse < w.standard.MSE || version.Rmse < w.standard.RMSE || version.R2 < w.standard.R2 {
		return false
	}
	return true
}

type OptionFunc func(options *Watcher)

func WithStandard(standard *types.ModelVersion) OptionFunc {
	return func(options *Watcher) {
		options.standard = standard
	}
}

func WithEnableAutoRefresh(EnableAutoRefresh bool) OptionFunc {
	return func(options *Watcher) {
		options.EnableAutoRefresh = EnableAutoRefresh
	}
}

func (w *Watcher) Stop() {
	close(w.done)
}

// NewWatcher watcher is responsible for receive model load signal, and then send the model to evaluator
func NewWatcher(mc client.Client, nv chan uint64, mv chan *types.ModelVersion, options ...OptionFunc) *Watcher {
	w := &Watcher{
		needVersion:  nv,
		modelVersion: mv,
		done:         make(chan struct{}),
		mc:           mc,
	}
	for _, opts := range options {
		opts(w)
	}
	return w
}
