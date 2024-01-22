/*
 *     Copyright 2023 The Dragonfly Authors
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

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	inference "d7y.io/api/v2/pkg/apis/inference"

	"d7y.io/dragonfly/v2/manager/models"
	"d7y.io/dragonfly/v2/manager/types"
	"d7y.io/dragonfly/v2/pkg/digest"
)

func (s *service) DestroyModel(ctx context.Context, id uint) error {
	model := models.Model{}
	if err := s.db.WithContext(ctx).First(&model, id).Error; err != nil {
		return err
	}

	// If the model is active, return an error.
	if model.State == models.ModelVersionStateActive {
		return errors.New("cannot delete an active model")
	}

	version, err := strconv.Atoi(model.Version)
	if err != nil {
		return err
	}

	if err := s.objectStorage.DeleteObject(ctx, s.config.Trainer.BucketName, types.MakeObjectKeyOfModelFile(model.Name, version)); err != nil {
		return err
	}

	if err := s.db.WithContext(ctx).Unscoped().Delete(&models.Model{}, id).Error; err != nil {
		return err
	}

	return nil
}

func (s *service) UpdateModel(ctx context.Context, id uint, json types.UpdateModelRequest) (*models.Model, error) {
	model := models.Model{}
	if err := s.db.WithContext(ctx).First(&model, id).Error; err != nil {
		return nil, err
	}

	// If the model is active, update the model config and
	// update the model state.
	if json.State == models.ModelVersionStateActive {
		if err := s.updateModelStateToActive(ctx, &model); err != nil {
			return nil, err
		}
	}

	// Update the model.
	if err := s.db.WithContext(ctx).Model(&model).Updates(models.Model{
		BIO: json.BIO,
	}).Error; err != nil {
		return nil, err
	}

	return &model, nil
}

func (s *service) GetModel(ctx context.Context, id uint) (*models.Model, error) {
	model := models.Model{}
	if err := s.db.WithContext(ctx).First(&model, id).Error; err != nil {
		return nil, err
	}

	return &model, nil
}

func (s *service) GetModels(ctx context.Context, q types.GetModelsQuery) ([]models.Model, int64, error) {
	var count int64
	var model []models.Model
	if err := s.db.WithContext(ctx).Scopes(models.Paginate(q.Page, q.PerPage)).Where(&models.Model{
		Type:        q.Type,
		Version:     q.Version,
		SchedulerID: q.SchedulerID,
	}).Find(&model).Limit(-1).Offset(-1).Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return model, count, nil
}

func (s *service) updateModelStateToActive(ctx context.Context, model *models.Model) error {
	version, err := strconv.ParseInt(model.Version, 10, 64)
	if err != nil {
		return err
	}

	// Update the model config to object storage.
	if err := s.updateModelConfig(ctx, model.Name, version); err != nil {
		return err
	}

	// Create a transaction to ensure that only one
	// version is active at a time.
	tx := s.db.WithContext(ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	if err := tx.Error; err != nil {
		return err
	}

	if err := tx.Model(&models.Model{}).Where(&models.Model{
		SchedulerID: model.SchedulerID,
		State:       models.ModelVersionStateActive,
	}).Updates(&models.Model{State: models.ModelVersionStateInactive}).Error; err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Model(model).Updates(&models.Model{State: models.ModelVersionStateActive}).Error; err != nil {
		tx.Rollback()
		return err
	}

	if tx.Commit().Error != nil {
		return err
	}

	return nil
}

func (s *service) updateModelConfig(ctx context.Context, name string, version int64) error {
	if !s.config.ObjectStorage.Enable {
		return errors.New("object storage is disabled")
	}

	objectKey := types.MakeObjectKeyOfModelConfigFile(name)
	var pbModelConfig inference.ModelConfig
	reader, err := s.objectStorage.GetOject(ctx, s.config.Trainer.BucketName, objectKey)
	if err != nil {
		return err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, &pbModelConfig); err != nil {
		return err
	}

	switch policyChoice := pbModelConfig.VersionPolicy.PolicyChoice.(type) {
	case *inference.ModelVersionPolicy_Specific_:
		// If the version already exists, add the version to the existing version list.
		policyChoice.Specific.Versions = []int64{version}
	default:
		return fmt.Errorf("unknown policy choice: %#v", policyChoice)
	}

	dgst := digest.New(digest.AlgorithmSHA256, digest.SHA256FromStrings(pbModelConfig.String()))
	if err := s.objectStorage.PutObject(ctx, s.config.Trainer.BucketName,
		types.MakeObjectKeyOfModelConfigFile(name), dgst.String(), strings.NewReader(pbModelConfig.String())); err != nil {
		return err
	}

	return nil
}
