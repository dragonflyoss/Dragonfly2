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

package training

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"

	tf "github.com/galeone/tensorflow/tensorflow/go"
)

const (
	// PartitionRatio is the partition ratio between the training set and the test set.
	PartitionRatio = 0.8

	// Batch size for each optimization step for MLP model.
	defaultMLPBatchSize int = 32

	// The number of epochs for training MLP model.
	defaultMLPEpochCount int = 10

	// The dimension of score Tensor for MLP model's inputs.
	defaultMLPScoreDims int = 5
)

// mlpTrainingPhase is MLP Training phase.
func mlpTrainingPhase(baseDir, hostID string, model *tf.SavedModel) error {
	// Open MLP observation training file.
	mlpObservationTrainFile, err := openMLPObservationTrainFile(baseDir, hostID)
	if err != nil {
		return err
	}
	defer mlpObservationTrainFile.Close()

	trainReader := csv.NewReader(mlpObservationTrainFile)
	for i := 0; i < defaultMLPEpochCount; i++ {
		for {
			// Read MLP observation training file by line.
			record, err := trainReader.Read()
			if err == io.EOF {
				// Reset MLP observation training file in line.
				mlpObservationTrainFile.Seek(0, 0)
				break
			}

			if err != nil {
				return err
			}

			var MLPBatchSize = defaultMLPBatchSize
			if len(record) < defaultMLPBatchSize {
				MLPBatchSize = len(record)
			}

			var (
				// Initialize score batch and target batch for training.
				scoresBatch = make([][]float64, 0, defaultMLPBatchSize)
				targetBatch = make([]float64, 0, defaultMLPBatchSize)
				scores      = make([]float64, defaultMLPBatchSize-1)
			)

			// Add scores and target of each MLP observation to score batch and target batch.
			for j, v := range record[:MLPBatchSize-1] {
				if scores[j], err = strconv.ParseFloat(v, 64); err != nil {
					return err
				}
			}
			scoresBatch = append(scoresBatch, scores)

			target, err := strconv.ParseFloat(record[MLPBatchSize-1], 64)
			if err != nil {
				return err
			}
			targetBatch = append(targetBatch, target)

			if len(scoresBatch) == MLPBatchSize {
				// Convert MLP observations to Tensorflow tensor.
				scoreTensor, err := tf.NewTensor(scoresBatch)
				if err != nil {
					return err
				}

				targetTensor, err := tf.NewTensor(targetBatch)
				if err != nil {
					return err
				}

				// Run an optimization step on the model using batch of values from observation file.
				if err := mlpLearning(model, scoreTensor, targetTensor); err != nil {
					return err
				}

				// Reset score batch and target batch for further training phase.
				scoresBatch = make([][]float64, 0, MLPBatchSize)
				targetBatch = make([]float64, 0, MLPBatchSize)
			}
		}
	}

	return nil
}

// mlpTestingPhase is MLP Testing phase.
func mlpTestingPhase(baseDir, hostID string, model *tf.SavedModel) (float64, float64, error) {
	var (
		accMSE    float64
		accMAE    float64
		testCount int
	)

	// Open MLP observation testing file.
	mlpObservationTestFile, err := openMLPObservationTestFile(baseDir, hostID)
	if err != nil {
		return 0, 0, err
	}
	defer mlpObservationTestFile.Close()

	testReader := csv.NewReader(mlpObservationTestFile)
	for {
		// Read preprocessed file in line.
		record, err := testReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			return 0, 0, err
		}

		// Initialize score and target for testing.
		var (
			scores       = make([][]float64, 1)
			target       = make([]float64, 1)
			recordLength = len(record)
		)
		scores[0] = make([]float64, recordLength-1)

		for j, v := range record[:recordLength-1] {
			val, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, 0, err
			}
			scores[0][j] = val
		}

		target[0], err = strconv.ParseFloat(record[recordLength-1], 64)
		if err != nil {
			return 0, 0, err
		}

		// Convert MLP observation to Tensorflow tensor.
		scoreTensor, err := tf.NewTensor(scores)
		if err != nil {
			return 0, 0, err
		}

		targetTensor, err := tf.NewTensor(target)
		if err != nil {
			return 0, 0, err
		}

		// Run test and receive avarage model metrics.
		MSE, MAE, err := mlpValidation(model, scoreTensor, targetTensor)
		if err != nil {
			return 0, 0, err
		}

		accMSE += MSE[0]
		accMAE += MAE[0]
		testCount++
	}

	MAE := accMAE / float64(testCount)
	MSE := accMSE / float64(testCount)
	return MAE, MSE, nil
}

// saveMLPModel saves and compress mlp model for uploading.
func saveMLPModel(baseDir, hostID string, model *tf.SavedModel) error {
	// Create MLP trained model directory file for updating and uploading.
	if err := os.MkdirAll(mlpModelDirFile(baseDir, hostID), os.ModePerm); err != nil {
		return err
	}

	// Create trained MLP model variables directory file.
	if err := os.MkdirAll(mlpModelVariablesFile(baseDir, hostID), os.ModePerm); err != nil {
		return err
	}

	mlpInitialSavedModelFile, err := openMLPInitialSavedModelFile()
	if err != nil {
		return err
	}
	defer mlpInitialSavedModelFile.Close()

	mlpSavedModelFile, err := openMLPSavedModelFile(baseDir, hostID)
	if err != nil {
		return err
	}
	defer mlpSavedModelFile.Close()

	// Copy MLP initial saved model file to MLP saved model file.
	if _, err := io.Copy(mlpSavedModelFile, mlpInitialSavedModelFile); err != nil {
		return err
	}

	// Update variables file.
	variableTensor, err := tf.NewTensor(mlpModelVariablesFile(baseDir, hostID))
	if err != nil {
		return err
	}

	if err := mlpSaving(model, variableTensor); err != nil {
		return err
	}

	return nil
}

// mlpValidation runs test set and receives avarage model metrics.
func mlpValidation(model *tf.SavedModel, scoreTensor, targetTensor *tf.Tensor) ([]float64, []float64, error) {
	// Receive MAE metric.
	res, err := model.Session.Run(
		map[tf.Output]*tf.Tensor{
			model.Graph.Operation("MAE_inputs").Output(0):  scoreTensor,
			model.Graph.Operation("MAE_targets").Output(0): targetTensor,
		},
		[]tf.Output{
			model.Graph.Operation("StatefulPartitionedCall").Output(0),
		},
		nil,
	)

	if err != nil {
		return nil, nil, err
	}
	mae := res[0].Value().([]float64)

	// Receive MSE metric.
	res, err = model.Session.Run(
		map[tf.Output]*tf.Tensor{
			model.Graph.Operation("MSE_inputs").Output(0):  scoreTensor,
			model.Graph.Operation("MSE_targets").Output(0): targetTensor,
		},
		[]tf.Output{
			model.Graph.Operation("StatefulPartitionedCall_1").Output(0),
		},
		nil,
	)

	if err != nil {
		return nil, nil, err
	}
	mse := res[0].Value().([]float64)

	return mse, mae, nil
}

// mlpLearning runs an optimization step on the model using batch of values from mlp observation file.
func mlpLearning(model *tf.SavedModel, scoreTensor, targetTensor *tf.Tensor) error {
	_, err := model.Session.Run(
		map[tf.Output]*tf.Tensor{
			model.Graph.Operation("train_inputs").Output(0):  scoreTensor,
			model.Graph.Operation("train_targets").Output(0): targetTensor,
		},
		[]tf.Output{
			model.Graph.Operation("StatefulPartitionedCall_3").Output(0),
			model.Graph.Operation("StatefulPartitionedCall_3").Output(1),
		},
		nil,
	)

	if err != nil {
		return err
	}

	return nil
}

// mlpSaving saves trained MLP model weight into files.
func mlpSaving(model *tf.SavedModel, variables *tf.Tensor) error {
	_, err := model.Session.Run(
		map[tf.Output]*tf.Tensor{
			model.Graph.Operation("saver_filename").Output(0): variables,
		},
		[]tf.Output{
			model.Graph.Operation("StatefulPartitionedCall_4").Output(0),
		},
		nil,
	)

	if err != nil {
		return err
	}

	return nil
}
