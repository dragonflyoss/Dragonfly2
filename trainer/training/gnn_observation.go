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

// createGNNVertexObservation inserts the GNN vertex observations into csv file.
func createGNNVertexObservation(baseDir, key string, observations ...GNNVertexObservation) error {
	file, err := os.OpenFile(gnnVertexObservationFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
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
func createGNNEdgeObservation(baseDir, key string, observations ...GNNEdgeObservation) error {
	file, err := os.OpenFile(gnnEdgeObservationFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := gocsv.MarshalWithoutHeaders(observations, file); err != nil {
		return err
	}

	return nil
}

// clearGNNVertexObservationFile removes gnn vertex observation file.
func clearGNNVertexObservationFile(baseDir, key string) error {
	return os.Remove(gnnVertexObservationFilename(baseDir, key))
}

// clearGNNEdgeObservationFile removes gnn edge observation file.
func clearGNNEdgeObservationFile(baseDir, key string) error {
	return os.Remove(gnnEdgeObservationFilename(baseDir, key))
}

// openGNNVertexObservationFile opens gnn vertex observation file for read based on the given model key, it returns io.ReadCloser of gnn vertex observation file.
func openGNNVertexObservationFile(baseDir, key string) (*os.File, error) {
	return os.OpenFile(gnnVertexObservationFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openGNNEdgeObservationFile opens gnn edge observation file for read based on the given model key, it returns io.ReadCloser of gnn edge observation file.
func openGNNEdgeObservationFile(baseDir, key string) (*os.File, error) {
	return os.OpenFile(gnnEdgeObservationFilename(baseDir, key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// gnnVertexObservationFilename generates gnn vertex observation file name based on the given key.
func gnnVertexObservationFilename(baseDir, key string) string {
	return filepath.Join(baseDir, fmt.Sprintf("%s_%s.%s", GNNVertexObservationFilePrefix, key, CSVFileExt))
}

// gnnEdgeObservationFilename generates gnn edge observation file name based on the given key.
func gnnEdgeObservationFilename(baseDir, key string) string {
	return filepath.Join(baseDir, fmt.Sprintf("%s_%s.%s", GNNEdgeObservationFilePrefix, key, CSVFileExt))
}
