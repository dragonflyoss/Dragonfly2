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

package types

type CreateV1PreheatRequest struct {
	Type                string            `json:"type" binding:"required,oneof=image file"`
	URL                 string            `json:"url" binding:"required"`
	FilteredQueryParams string            `json:"filteredQueryParams" binding:"omitempty"`
	Headers             map[string]string `json:"headers" binding:"omitempty"`
}

type CreateV1PreheatResponse struct {
	ID string `json:"id"`
}

type GetV1PreheatResponse struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	StartTime  string `json:"startTime,omitempty"`
	FinishTime string `json:"finishTime,omitempty"`
}

type V1PreheatParams struct {
	ID string `uri:"id" binding:"required"`
}
