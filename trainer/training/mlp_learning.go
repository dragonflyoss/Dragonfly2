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

import tf "github.com/galeone/tensorflow/tensorflow/go"

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
