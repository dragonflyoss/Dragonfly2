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

// Package stringutils provides utilities supplementing the standard 'strings' package.
package stringutils

import (
	"math/rand"
	"strings"
	"time"
	"unicode"
)

func SubString(str string, start, end int) string {
	runes := []rune(str)
	length := len(runes)
	if start < 0 || start >= length || end <= 0 || end > length || start >= end {
		return ""
	}

	return string(runes[start:end])
}

func IsBlank(str string) bool {
	for _, c := range str {
		if !unicode.IsSpace(c) {
			return false
		}
	}

	return true
}

func IsNotBlank(str string) bool {
	return !IsBlank(str)
}

func IsEmpty(str string) bool {
	return str == ""
}

func ContainsFold(slice []string, ele string) bool {
	for _, one := range slice {
		if strings.EqualFold(one, ele) {
			return true
		}
	}

	return false
}

func Contains(slice []string, ele string) bool {
	for _, one := range slice {
		if one == ele {
			return true
		}
	}

	return false
}

func RandString(len int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}
