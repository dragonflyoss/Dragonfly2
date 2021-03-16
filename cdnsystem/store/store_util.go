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

package store

import (
	"d7y.io/dragonfly/v2/cdnsystem/cdnerrors"
	"github.com/pkg/errors"
)

// CheckGetRaw check before get Raw
func CheckGetRaw(raw *Raw, fileLength int64) error {
	if fileLength < raw.Offset {
		return errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "the offset: %d is lager than the file length: %d", raw.Offset, fileLength)
	}

	if raw.Length < 0 {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "the length: %d is not a positive integer", raw.Length)
	}

	if fileLength < (raw.Offset + raw.Length) {
		return errors.Wrapf(cdnerrors.ErrRangeNotSatisfiable, "the offset: %d and length: %d is lager than the file length: %d", raw.Offset, raw.Length, fileLength)
	}
	return nil
}

// CheckPutRaw check before put Raw
func CheckPutRaw(raw *Raw) error {
	if raw.Length < 0 {
		return errors.Wrapf(cdnerrors.ErrInvalidValue, "the length: %d should not be a negative integer", raw.Length)
	}
	return nil
}
