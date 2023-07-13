/*
 *     Copyright 2022 The Dragonfly Authors
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

package storage

// MLPObservation contains content for the observed data for download file
type MLPObservation struct {
	// FinishedPieceScore is feature, 0.0~unlimited larger and better.
	FinishedPieceScore float64 `csv:"finishedPieceScore"`

	// FreeUploadScore is feature, 0.0~unlimited larger and better.
	FreeUploadScore float64 `csv:"freeUploadScore"`

	// UploadSuccessScore is feature, 0.0~unlimited larger and better.
	UploadSuccessScore float64 `csv:"uploadPieceCount"`

	// IDCAffinityScore is feature, 0.0~unlimited larger and better.
	IDCAffinityScore float64 `csv:"idcAffinityScore"`

	// LocationAffinityScore is feature, 0.0~unlimited larger and better.
	LocationAffinityScore float64 `csv:"locationAffinityScore"`

	// Bandwidth is label, calculated by length and cost.
	Bandwidth float64 `csv:"cost"`
}

// TODO
// GNNObservation contains content for the observed data for network topology file.
type GNNObservation struct {
}
