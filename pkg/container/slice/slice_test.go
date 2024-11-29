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

package slice

import (
	"reflect"
	"sort"
	"testing"
)

// TestKeyBy tests the KeyBy function
func TestKeyBy(t *testing.T) {
	type Person struct {
		ID   int
		Name string
	}

	people := []Person{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	// Test case 1: Key by ID
	keyByID := KeyBy(people, func(p Person) int {
		return p.ID
	})
	expectedKeyByID := map[int]Person{
		1: {ID: 1, Name: "Alice"},
		2: {ID: 2, Name: "Bob"},
		3: {ID: 3, Name: "Charlie"},
	}
	if !reflect.DeepEqual(keyByID, expectedKeyByID) {
		t.Errorf("KeyBy by ID failed, expected %v, got %v", expectedKeyByID, keyByID)
	}

	// Test case 2: Key by Name
	keyByName := KeyBy(people, func(p Person) string {
		return p.Name
	})
	expectedKeyByName := map[string]Person{
		"Alice":   {ID: 1, Name: "Alice"},
		"Bob":     {ID: 2, Name: "Bob"},
		"Charlie": {ID: 3, Name: "Charlie"},
	}
	if !reflect.DeepEqual(keyByName, expectedKeyByName) {
		t.Errorf("KeyBy by Name failed, expected %v, got %v", expectedKeyByName, keyByName)
	}
}

// TestValues tests the Values function
func TestValues(t *testing.T) {
	map1 := map[int]string{
		1: "one",
		2: "two",
	}
	map2 := map[int]string{
		3: "three",
		4: "four",
	}

	// Test case 1: Values from one map
	values1 := Values(map1)
	expectedValues1 := []string{"one", "two"}

	sort.Strings(values1)
	sort.Strings(expectedValues1)
	if !reflect.DeepEqual(values1, expectedValues1) {
		t.Errorf("Values from one map failed, expected %v, got %v", expectedValues1, values1)
	}

	// Test case 2: Values from multiple maps
	values2 := Values(map1, map2)
	expectedValues2 := []string{"one", "two", "three", "four"}

	sort.Strings(values2)
	sort.Strings(expectedValues2)
	if !reflect.DeepEqual(values2, expectedValues2) {
		t.Errorf("Values from multiple maps failed, expected %v, got %v", expectedValues2, values2)
	}

	// Test case 3: Values from empty maps
	values3 := Values(map[int]string{}, map[int]string{})
	expectedValues3 := []string{}
	if !reflect.DeepEqual(values3, expectedValues3) {
		t.Errorf("Values from empty maps failed, expected %v, got %v", expectedValues3, values3)
	}
}
