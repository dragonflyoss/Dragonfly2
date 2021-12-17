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

package main

import (
	"fmt"
	"io"
	"os"

	"d7y.io/dragonfly/v2/pkg/source"
)

func main() {
	client, err := source.LoadPlugin("./testdata", "dfs")
	if err != nil {
		fmt.Printf("load plugin error: %s\n", err)
		os.Exit(1)
	}

	request, err := source.NewRequest("")
	l, err := client.GetContentLength(request)
	if err != nil {
		fmt.Printf("get content length error: %s\n", err)
		os.Exit(1)
	}

	request, err = source.NewRequest("")
	response, err := client.Download(request)
	if err != nil {
		fmt.Printf("download error: %s\n", err)
		os.Exit(1)
	}

	data, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Printf("read error: %s\n", err)
		os.Exit(1)
	}

	if l != int64(len(data)) {
		fmt.Printf("content length mismatch\n")
		os.Exit(1)
	}

	err = response.Body.Close()
	if err != nil {
		fmt.Printf("close error: %s\n", err)
		os.Exit(1)
	}
}
