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
	"fmt"
	"os"
	"path/filepath"

	"github.com/gocarina/gocsv"
)

const (
	// MLPObservationTrainFilePrefix is prefix of mlp observation file name for mlp training.
	MLPObservationTrainFilePrefix = "mlp_train_data"

	// MLPObservationTestFilePrefix is prefix of mlp test observation file name for mlp testing.
	MLPObservationTestFilePrefix = "mlp_test_data"

	// Initial model path for training MLP model.
	MLPInitialModelPath = "../models/mlp"

	// MLPModelFilePrefix is the prefix of trained MLP model.
	MLPModelFilePrefix = "mlp_train"
)

const (
	// CSVFileExt is extension of file name.
	CSVFileExt = "csv"

	// Variables is variable directory name.
	VariablesDir = "variables"

	// SavedModelFilename is saved model file name.
	SavedModelFilename = "saved_model.pb"
)

// createMLPObservationTrain inserts the MLP observations into csv file for training.
func createMLPObservationTrain(baseDir, key string, observations ...MLPObservation) error {
	file, err := os.OpenFile(mlpObservationTrainFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// createMLPObservationTest inserts the MLP observations into csv file for testing.
func createMLPObservationTest(baseDir, key string, observations ...MLPObservation) error {
	file, err := os.OpenFile(mlpObservationTestFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// clearMLPObservationTrainFile removes mlp train observation file.
func clearMLPObservationTrainFile(baseDir, key string) error {
	return os.Remove(mlpObservationTrainFilename(baseDir, key))
}

// clearMLPObservationTestFile removes mlp test observation file.s
func clearMLPObservationTestFile(baseDir, key string) error {
	return os.Remove(mlpObservationTestFilename(baseDir, key))
}

// openMLPObservationTrainFile opens mlp observation train file for read based on the given model key, it returns io.ReadCloser of mlp observation train file.
func openMLPObservationTrainFile(baseDir, key string) (*os.File, error) {
	return os.OpenFile(mlpObservationTrainFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openMLPObservationTestFile opens mlp observation test file for read based on the given model key, it returns io.ReadCloser of mlp observation test file.
func openMLPObservationTestFile(baseDir, key string) (*os.File, error) {
	return os.OpenFile(mlpObservationTestFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openMLPInitialSavedModelFile opens mlp initial saved model file for read based on the given model key, it returns io.ReadCloser of mlp saved model file.
func openMLPInitialSavedModelFile() (*os.File, error) {
	return os.OpenFile(mlpInitialSavedModelFile(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openMLPSavedModelFile opens mlp saved model file for read based on the given model key, it returns io.ReadCloser of mlp saved model file.
func openMLPSavedModelFile(baseDir, key string) (*os.File, error) {
	return os.OpenFile(mlpSavedModelFile(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// mlpObservationTrainFilename generates mlp observation train file name based on the given key.
func mlpObservationTrainFilename(baseDir, key string) string {
	return filepath.Join(fmt.Sprintf("%s_%s.%s", MLPObservationTrainFilePrefix, key, CSVFileExt))
}

// mlpObservationTestFilename generates mlp observation test file name based on the given key.
func mlpObservationTestFilename(baseDir, key string) string {
	return filepath.Join(fmt.Sprintf("%s_%s.%s", MLPObservationTestFilePrefix, key, CSVFileExt))
}

// mlpModelDirFile generates trained mlp model directory file name based on the given key.
func mlpModelDirFile(baseDir, key string) string {
	return filepath.Join(baseDir, fmt.Sprintf("%s_%s", MLPModelFilePrefix, key))
}

// mlpInitialSavedModelFile generates trained MLP initial saved model file name.
func mlpInitialSavedModelFile() string {
	return filepath.Join(filepath.Join(MLPInitialModelPath, SavedModelFilename), SavedModelFilename)
}

// mlpSavedModelFile generates trained MLP saved model name based on the given key.
func mlpSavedModelFile(baseDir, key string) string {
	return filepath.Join(mlpModelDirFile(baseDir, key), SavedModelFilename)
}

// mlpModelVariablesFile generates trained MLP model variables directory name based on the given key.
func mlpModelVariablesFile(baseDir, key string) string {
	return filepath.Join(mlpModelDirFile(baseDir, key), VariablesDir)
}
