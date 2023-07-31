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

	// GNNVertexObservationFilePrefix is gnn vertex observation of file name.
	GNNVertexObservationFilePrefix = "gnn_vertex_data"

	// GNNEdgeObservationFilePrefix is prefix of gnn edge observation file name.
	GNNEdgeObservationFilePrefix = "gnn_edge_data"

	// CSVFileExt is extension of file name.
	CSVFileExt = "csv"
)

// observation provides observation function.
type observation struct {
	baseDir string
}

// NewObservation returns a new observation instance.
func NewObservation(baseDir string) *observation {
	return &observation{baseDir: baseDir}
}

// createMLPObservationTrain inserts the MLP observations into csv file for training.
func (o *observation) createMLPObservationTrain(key string, observations ...MLPObservation) error {
	file, err := os.OpenFile(o.mlpObservationTrainFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
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
func (o *observation) createMLPObservationTest(key string, observations ...MLPObservation) error {
	file, err := os.OpenFile(o.mlpObservationTestFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// createGNNVertexObservation inserts the GNN vertex observations into csv file.
func (o *observation) createGNNVertexObservation(key string, observations ...GNNVertexObservation) error {
	file, err := os.OpenFile(o.gnnVertexObservationFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// createGNNEdgeObservation inserts the GNN edge observations into csv file.
func (o *observation) createGNNEdgeObservation(key string, observations ...GNNEdgeObservation) error {
	file, err := os.OpenFile(o.gnnEdgeObservationFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
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
func (o *observation) clearMLPObservationTrainFile(key string) error {
	return os.Remove(o.mlpObservationTrainFilename(key))
}

// clearMLPObservationTestFile removes mlp test observation file.
func (o *observation) clearMLPObservationTestFile(key string) error {
	return os.Remove(o.mlpObservationTestFilename(key))
}

// clearGNNVertexObservationFile removes gnn vertex observation file.
func (o *observation) clearGNNVertexObservationFile(key string) error {
	return os.Remove(o.gnnVertexObservationFilename(key))
}

// clearGNNEdgeObservationFile removes gnn edge observation file.
func (o *observation) clearGNNEdgeObservationFile(key string) error {
	return os.Remove(o.gnnEdgeObservationFilename(key))
}

// openMLPObservationTrainFile opens mlp observation train file for read based on the given model key, it returns io.ReadCloser of mlp observation train file.
func (o *observation) openMLPObservationTrainFile(key string) (*os.File, error) {
	return os.OpenFile(o.mlpObservationTrainFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openMLPObservationTestFile opens mlp observation test file for read based on the given model key, it returns io.ReadCloser of mlp observation test file.
func (o *observation) openMLPObservationTestFile(key string) (*os.File, error) {
	return os.OpenFile(o.mlpObservationTestFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openGNNVertexObservationFile opens gnn vertex observation file for read based on the given model key, it returns io.ReadCloser of gnn vertex observation file.
func (o *observation) openGNNVertexObservationFile(key string) (*os.File, error) {
	return os.OpenFile(o.gnnVertexObservationFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openGNNEdgeObservationFile opens gnn edge observation file for read based on the given model key, it returns io.ReadCloser of gnn edge observation file.
func (o *observation) openGNNEdgeObservationFile(key string) (*os.File, error) {
	return os.OpenFile(o.gnnEdgeObservationFilename(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// mlpObservationTrainFilename generates mlp observation train file name based on the given key.
func (o *observation) mlpObservationTrainFilename(key string) string {
	return filepath.Join(o.baseDir, fmt.Sprintf("%s_%s.%s", MLPObservationTrainFilePrefix, key, CSVFileExt))
}

// mlpObservationTestFilename generates mlp observation test file name based on the given key.
func (o *observation) mlpObservationTestFilename(key string) string {
	return filepath.Join(o.baseDir, fmt.Sprintf("%s_%s.%s", MLPObservationTestFilePrefix, key, CSVFileExt))
}

// gnnVertexObservationFilename generates gnn vertex observation file name based on the given key.
func (o *observation) gnnVertexObservationFilename(key string) string {
	return filepath.Join(o.baseDir, fmt.Sprintf("%s_%s.%s", GNNVertexObservationFilePrefix, key, CSVFileExt))
}

// gnnEdgeObservationFilename generates gnn edge observation file name based on the given key.
func (o *observation) gnnEdgeObservationFilename(key string) string {
	return filepath.Join(o.baseDir, fmt.Sprintf("%s_%s.%s", GNNEdgeObservationFilePrefix, key, CSVFileExt))
}
