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
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// Initial model path for training MLP model.
	MLPInitialModelPath = "../models/MLP"

	// MLPModelFilePrefix is the prefix of trained MLP model.
	MLPModelFilePrefix = "MLP_trained"

	// Variables is variable directory name.
	VariablesDir = "variables"

	// SavedModelFilename is saved model file name.
	SavedModelFilename = "saved_model.pb"
)

// files provides files function.
type files struct {
	baseDir string
}

// NewFiles returns a new files instance.
func NewFiles(baseDir string) *files {
	return &files{baseDir: baseDir}
}

// mlpModelDirFile generates trained mlp model directory file name based on the given key.
func (f *files) mlpModelDirFile(key string) string {
	return filepath.Join(f.baseDir, fmt.Sprintf("%s_%s", MLPModelFilePrefix, key))
}

// mlpInitialSavedModelFile generates trained MLP initial saved model file name.
func (f *files) mlpInitialSavedModelFile() string {
	return filepath.Join(filepath.Join(MLPInitialModelPath, SavedModelFilename), SavedModelFilename)
}

// mlpSavedModelFile generates trained MLP saved model name based on the given key.
func (f *files) mlpSavedModelFile(key string) string {
	return filepath.Join(f.mlpModelDirFile(key), SavedModelFilename)
}

// mlpModelVariablesFile generates trained MLP model variables directory name based on the given key.
func (f *files) mlpModelVariablesFile(key string) string {
	return filepath.Join(f.mlpModelDirFile(key), VariablesDir)
}

// openMLPInitialSavedModelFile opens mlp initial saved model file for read based on the given model key, it returns io.ReadCloser of mlp saved model file.
func (f *files) openMLPInitialSavedModelFile() (*os.File, error) {
	return os.OpenFile(f.mlpInitialSavedModelFile(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// openMLPSavedModelFile opens mlp saved model file for read based on the given model key, it returns io.ReadCloser of mlp saved model file.
func (f *files) openMLPSavedModelFile(key string) (*os.File, error) {
	return os.OpenFile(f.mlpSavedModelFile(key), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
}

// compressMLPDir compresses MLP trained model directory and convert to bytes.
func compressMLPDir(dir string) ([]byte, error) {
	// create a gzip compressor.
	buf := new(bytes.Buffer)
	gzWriter := gzip.NewWriter(buf)
	defer gzWriter.Close()

	// create a zip compressor.
	zipWriter := zip.NewWriter(gzWriter)
	defer zipWriter.Close()

	// iterate through all the files and subdirectories in the directory and add them to the zip compressor.
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// create a zip file header.
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// set the zip file header name.
		header.Name = path

		// If the current file is a directory, add the zip file header directly.
		if info.IsDir() {
			_, err = zipWriter.CreateHeader(header)
			return err
		}

		// If the current file is a normal file, open it and add its contents to the zip compressor.
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		entry, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}

		_, err = io.Copy(entry, file)
		return err
	}); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
