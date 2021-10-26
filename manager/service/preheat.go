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

package service

import (
	"context"
	"strconv"

	logger "d7y.io/dragonfly/v2/internal/dflog"
	internaljob "d7y.io/dragonfly/v2/internal/job"
	"d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	machineryv1tasks "github.com/RichardKnop/machinery/v1/tasks"
)

const (
	// V1PreheatingStatusPending is the preheating is waiting for starting
	V1PreheatingStatusPending = "WAITING"

	// V1PreheatingStatusRunning is the preheating is running
	V1PreheatingStatusRunning = "RUNNING"

	// V1PreheatingStatusSuccess is the preheating is success
	V1PreheatingStatusSuccess = "SUCCESS"

	// V1PreheatingStatusFail is the preheating is failed
	V1PreheatingStatusFail = "FAIL"
)

func (s *rest) CreateV1Preheat(ctx context.Context, json types.CreateV1PreheatRequest) (*types.CreateV1PreheatResponse, error) {
	job, err := s.CreatePreheatJob(ctx, types.CreatePreheatJobRequest{
		Type: internaljob.PreheatJob,
		Args: types.PreheatArgs{
			Type:    json.Type,
			URL:     json.URL,
			Filter:  json.Filter,
			Headers: json.Headers,
		},
	})
	if err != nil {
		return nil, err
	}

	return &types.CreateV1PreheatResponse{
		ID: strconv.FormatUint(uint64(job.ID), 10),
	}, nil
}

func (s *rest) GetV1Preheat(ctx context.Context, rawID string) (*types.GetV1PreheatResponse, error) {
	id, err := strconv.ParseUint(rawID, 10, 32)
	if err != nil {
		logger.Errorf("preheat convert error", err)
	}

	job := model.Job{}
	if err := s.db.WithContext(ctx).First(&job, uint(id)).Error; err != nil {
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &types.GetV1PreheatResponse{
		ID:         strconv.FormatUint(uint64(job.ID), 10),
		Status:     convertStatus(job.Status),
		StartTime:  job.CreatedAt.String(),
		FinishTime: job.UpdatedAt.String(),
	}, nil
}

func convertStatus(status string) string {
	switch status {
	case machineryv1tasks.StatePending, machineryv1tasks.StateReceived, machineryv1tasks.StateRetry:
		return V1PreheatingStatusPending
	case machineryv1tasks.StateStarted:
		return V1PreheatingStatusRunning
	case machineryv1tasks.StateSuccess:
		return V1PreheatingStatusSuccess
	case machineryv1tasks.StateFailure:
		return V1PreheatingStatusFail
	}

	return V1PreheatingStatusFail
}
