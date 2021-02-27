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

package comparator

import (
	"reflect"
	"strconv"
)

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func IsNil(value interface{}) bool {
	if value == nil {
		return true
	}

	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Slice:
		return v.IsNil()
	}

	return false
}

func IsZero(value interface{}) bool {
	if value == nil {
		return true
	}
	return reflect.ValueOf(value).IsZero()
}

func IsTrue(value bool) bool {
	return value
}

func IsPositive(value int64) bool {
	return value > 0
}

func IsNatural(value string) bool {
	if v, err := strconv.ParseInt(value, 0, 64); err == nil {
		return v >= 0
	}
	return false
}

func IsInteger(value string) bool {
	if _, err := strconv.ParseInt(value, 0, 64); err == nil {
		return true
	}
	return false
}
